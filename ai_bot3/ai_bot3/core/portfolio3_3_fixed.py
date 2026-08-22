
import asyncio
import gc
import logging
# 在程序的最开始设置"spawn"模式
import multiprocessing as mp
import os
import signal
import sys
import threading
import time
import shutil
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Set, Optional as Opt

import psutil

try:
    mp.set_start_method("spawn", force=True)
except RuntimeError:
    pass

# 读取 RLIMIT_NOFILE 软限制（用于自适应 FD 水位线）
try:
    import resource as _resource  # POSIX 专属
    _RLIMIT_NOFILE_SOFT, _RLIMIT_NOFILE_HARD = _resource.getrlimit(_resource.RLIMIT_NOFILE)
except Exception:  # pragma: no cover - 非 POSIX 兜底
    _RLIMIT_NOFILE_SOFT, _RLIMIT_NOFILE_HARD = 1024, 1024

from .data_fetch import DataFetcher
from .http_client import HTTPClient
from .result_manager import ResultManager
from .sentiment import Sentiment
from .market_context import OpenAIFormatSignalClient
from .online_calibration import OnlinePredictionCalibrator
from .brain_model import train_brain_from_df
from .kline_feature_store import FeatureStoreIntegrityError, KlineFeatureStore

# 活跃训练 / 推理路径必须使用 *_fixed 分支
from .trainer3 import TrainerDataPreparer, run_training_in_process
from .inferencer3_fixed import InferencerDataPreparer, run_keras_inference_in_process

RESULTS_DIR = Path("model_results")
RESULTS_DIR.mkdir(exist_ok=True)
rm = ResultManager(RESULTS_DIR)

# =============== 进程池管理配置（保持 3-3 语义，增加鲁棒性） ===============
PROCESS_POOL: Optional[ProcessPoolExecutor] = None
MEMORY_THRESHOLD_PERCENT = float(os.environ.get("MEMORY_THRESHOLD_PERCENT", "88"))  # 系统内存阈值
MAX_WORKERS = 1                # 严格 1 个子进程，稳定优先
SHUTDOWN_TIMEOUT = 15
MAX_MEMORY_PER_TASK_MB = int(os.environ.get("MAX_MEMORY_PER_TASK_MB", "4000"))
DEFAULT_TASK_TIMEOUT_PREDICT = int(os.environ.get("PREDICT_TASK_TIMEOUT", "300"))    # 预测默认 5 分钟
DEFAULT_TASK_TIMEOUT_TRAIN = int(os.environ.get("TRAIN_TASK_TIMEOUT", "7200"))     # 训练默认 2 小时（可从配置覆盖）
CLEANUP_WAIT_TIME = 3

# —— FD 水位线（避免 Errno 24） ——
# 自适应默认值必须基于真实 RLIMIT_NOFILE。线上进程稳定态可能已有 900+ FD，
# 不能因为旧的 819/942 水位线把每次训练/预测都跳过。
def _effective_nofile_soft() -> int:
    try:
        soft = int(_RLIMIT_NOFILE_SOFT)
        if soft <= 0:
            return 1024
        # resource.RLIM_INFINITY 在部分系统上是极大整数，给一个可控上限。
        return min(soft, 1048576)
    except Exception:
        return 1024


def _default_fd_watermark() -> int:
    soft = _effective_nofile_soft()
    if soft <= 1024:
        # 小 ulimit 环境下只在非常接近上限时预警；944/1024 不应直接跳过。
        return max(256, min(soft - 96, int(soft * 0.88)))
    if soft <= 2048:
        return max(512, min(soft - 128, int(soft * 0.82)))
    return max(2048, min(int(soft * 0.70), soft - 512))


def _default_fd_hard_limit(watermark: int) -> int:
    soft = _effective_nofile_soft()
    if soft <= 1024:
        return max(watermark + 32, min(soft - 16, int(soft * 0.97)))
    if soft <= 2048:
        return max(watermark + 64, min(soft - 32, int(soft * 0.95)))
    return max(watermark + 256, min(soft - 128, int(soft * 0.92)))


_FD_WATERMARK_DEFAULT = _default_fd_watermark()
_env_fd = os.environ.get("FD_WATERMARK")
if _env_fd:
    try:
        FD_WATERMARK = int(_env_fd)
    except Exception:
        FD_WATERMARK = _FD_WATERMARK_DEFAULT
else:
    FD_WATERMARK = _FD_WATERMARK_DEFAULT

# clamp env/default to a sane range under the actual soft limit
_soft_for_fd = _effective_nofile_soft()
FD_WATERMARK = max(128, min(FD_WATERMARK, max(128, _soft_for_fd - 64)))
FD_HARD_LIMIT = _default_fd_hard_limit(FD_WATERMARK)
_env_fd_hard = os.environ.get("FD_HARD_LIMIT")
if _env_fd_hard:
    try:
        FD_HARD_LIMIT = int(_env_fd_hard)
    except Exception:
        pass
FD_HARD_LIMIT = max(FD_WATERMARK + 16, min(FD_HARD_LIMIT, max(FD_WATERMARK + 16, _soft_for_fd - 16)))
# 长期运行自愈阈值：达到该值时退出当前 train/forecast 进程，让 run_v3.sh supervisor 拉起干净进程。
# 这样不会无限跳过任务；即使未来 FD 泄漏，也会自动恢复。
_env_fd_restart = os.environ.get("FD_RESTART_LIMIT")
try:
    FD_RESTART_LIMIT = int(_env_fd_restart) if _env_fd_restart else min(max(FD_HARD_LIMIT - 512, FD_WATERMARK + 128), max(_soft_for_fd - 256, FD_WATERMARK + 128))
except Exception:
    FD_RESTART_LIMIT = min(max(FD_HARD_LIMIT - 512, FD_WATERMARK + 128), max(_soft_for_fd - 256, FD_WATERMARK + 128))
FD_RESTART_LIMIT = max(FD_WATERMARK + 16, min(FD_RESTART_LIMIT, max(FD_WATERMARK + 16, _soft_for_fd - 32)))
_fd_limits_logged = False

WORKER_WATCHDOG_INTERVAL = int(os.environ.get("WORKER_WATCHDOG_INTERVAL", "15"))
DEFAULT_MODE_TIMEOUT_PREDICT = int(os.environ.get("PREDICT_MODE_TIMEOUT", "1800"))
DEFAULT_MODE_TIMEOUT_TRAIN = int(os.environ.get("TRAIN_MODE_TIMEOUT", "21600"))

_pool_lock = threading.Lock()
_pool_creation_time = 0
_consecutive_failures = 0
_last_cleanup_time = 0

class PoolState:
    IDLE = "idle"
    BUSY = "busy"
    CLEANING = "cleaning"
    FAILED = "failed"

_pool_state = PoolState.IDLE

# ========================= 工具函数：内存/进程/文件 =========================
def force_cleanup_memory():
    """强制清理内存"""
    try:
        if os.environ.get("AI_BOT_FORCE_CPU") == "1" or os.environ.get("AI_BOT_ALLOW_PARENT_TF_CLEANUP") == "1":
            try:
                import keras
                import tensorflow as tf
                keras.backend.clear_session()
                if hasattr(tf.keras.backend, 'clear_session'):
                    tf.keras.backend.clear_session()
            except ImportError:
                pass
            except Exception as exc:
                logging.debug(f"TensorFlow cleanup skipped after error: {exc}")
        else:
            logging.debug("Skip TensorFlow parent cleanup to avoid CUDA init before spawned workers")

        for _ in range(3):
            collected = gc.collect()
            if collected == 0:
                break

        logging.debug("内存清理完成")
    except Exception as e:
        logging.warning(f"内存清理时出现异常: {e}")

def _num_open_fds() -> int:
    try:
        p = psutil.Process()
        if hasattr(p, "num_fds"):
            return p.num_fds()
        # Windows 近似值
        return len(p.open_files()) + len(p.connections())
    except Exception:
        return -1


def _request_process_restart(reason: str) -> None:
    """主动退出当前 train/forecast 进程，由 run_v3.sh supervisor 自动重启。"""
    logging.error(f"[self-heal] {reason}; 主动退出进程触发 supervisor 重启")
    try:
        shutdown_process_pool(force=True, wait_for_completion=False)
    except Exception:
        pass
    try:
        kill_zombie_processes(terminate_live=True)
    except Exception:
        pass
    os._exit(75)

def kill_zombie_processes(*, terminate_live: bool = False):
    """Reap this process's children without touching another service's workers.

    Normal maintenance only reaps zombies.  Live descendants are terminated solely
    during an explicit process-pool shutdown/restart.
    """
    global _last_cleanup_time

    current_time = time.time()
    if current_time - _last_cleanup_time < 5:
        return
    _last_cleanup_time = current_time

    current_pid = os.getpid()
    killed_count = 0
    zombie_count = 0

    def reap_zombies():
        nonlocal zombie_count
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
                logging.debug(f"回收僵尸进程 PID {pid}")
                zombie_count += 1
            except OSError:
                break

    try:
        old_handler = signal.signal(signal.SIGCHLD, lambda s, f: reap_zombies())
        reap_zombies()
        signal.signal(signal.SIGCHLD, old_handler)
    except Exception as e:
        logging.warning(f"僵尸进程清理失败: {e}")

    try:
        parent = psutil.Process(current_pid)
        descendants = parent.children(recursive=True)
        live_children = []
        for child in descendants:
            try:
                if child.status() == psutil.STATUS_ZOMBIE:
                    try:
                        child.wait(timeout=0)
                    except (psutil.TimeoutExpired, psutil.NoSuchProcess):
                        pass
                    zombie_count += 1
                elif terminate_live:
                    child.terminate()
                    live_children.append(child)
                    killed_count += 1
                    logging.debug(f"终止本服务子进程 PID {child.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        if live_children:
            _, alive = psutil.wait_procs(live_children, timeout=2)
            for child in alive:
                try:
                    child.kill()
                    logging.warning(f"强制杀死本服务子进程 PID {child.pid}")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
    except Exception as e:
        logging.error(f"清理子进程时出错: {e}")

    if killed_count > 0 or zombie_count > 0:
        logging.info(f"清理了 {killed_count} 个活跃进程和 {zombie_count} 个僵尸进程")

def get_process_pool() -> ProcessPoolExecutor:
    """获取或创建进程池"""
    global PROCESS_POOL, _pool_creation_time, _consecutive_failures, _pool_state

    with _pool_lock:
        current_time = time.time()
        needs_new_pool = (
            PROCESS_POOL is None or
            getattr(PROCESS_POOL, '_shutdown', False) or
            getattr(PROCESS_POOL, '_shutdown_thread', False) or
            _pool_state == PoolState.FAILED
        )
        if needs_new_pool:
            if _consecutive_failures > 3:
                wait_time = max(_consecutive_failures * 2, 10)
                logging.warning(f"连续失败 {_consecutive_failures} 次，等待 {wait_time} 秒后重新创建进程池")
                time.sleep(wait_time)
            time.sleep(10)

            logging.info("创建新的进程池...")
            try:
                PROCESS_POOL = ProcessPoolExecutor(
                    max_workers=MAX_WORKERS,
                    mp_context=mp.get_context('spawn')
                )
                _pool_creation_time = current_time
                _pool_state = PoolState.IDLE
                _consecutive_failures = 0
                logging.info(f"进程池已创建，最大工作进程数: {MAX_WORKERS}")
            except Exception as e:
                logging.error(f"创建进程池失败: {e}")
                _consecutive_failures += 1
                _pool_state = PoolState.FAILED
                raise
        return PROCESS_POOL

def shutdown_process_pool(force: bool = False, wait_for_completion: bool = True):
    """关闭进程池"""
    global PROCESS_POOL, _pool_state
    with _pool_lock:
        if PROCESS_POOL is not None:
            try:
                _pool_state = PoolState.CLEANING
                logging.info(f"开始{'强制' if force else '优雅'}关闭进程池")
                if force:
                    try:
                        PROCESS_POOL.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        PROCESS_POOL.shutdown(wait=False)
                    if wait_for_completion:
                        time.sleep(1)
                        kill_zombie_processes(terminate_live=True)
                else:
                    start_time = time.time()
                    PROCESS_POOL.shutdown(wait=True)
                    elapsed = time.time() - start_time
                    if elapsed > SHUTDOWN_TIMEOUT:
                        logging.warning(f"优雅关闭超时 ({elapsed:.1f}s)")
                logging.info("进程池已关闭")
            except Exception as e:
                logging.error(f"关闭进程池时出错: {e}")
                if wait_for_completion:
                    kill_zombie_processes()
            finally:
                PROCESS_POOL = None
                _pool_state = PoolState.IDLE

def is_process_pool_healthy(pool: ProcessPoolExecutor) -> bool:
    """检查进程池是否健康"""
    if pool is None:
        return False
    try:
        if hasattr(pool, '_shutdown') and pool._shutdown:
            return False
        if hasattr(pool, '_shutdown_thread') and pool._shutdown_thread:
            return False
        global _pool_creation_time
        current_time = time.time()
        if current_time - _pool_creation_time > 3600:  # 超过 1 小时重建
            logging.info("进程池运行时间过长，标记为不健康")
            return False
        return True
    except Exception:
        return False

def _atomic_copy(src: Path, dst: Path, log: logging.Logger):
    """原子复制：先写入 .tmp，再 os.replace 到目标"""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)
    log.debug(f"[atomic_copy] {src.name} -> {dst}")

def _is_valid_file(p: Path, min_kb: int = 64) -> bool:
    """检查文件是否存在并满足最小大小要求；pkl 放宽到 1KB"""
    try:
        if not p.exists():
            return False
        min_bytes = 1024 if p.suffix == ".pkl" else min_kb * 1024
        return p.stat().st_size >= min_bytes
    except Exception:
        return False

# ========================= 执行器（保持 3-3 语义，定向修复） =========================
async def execute_with_memory_limit(func, *args, timeout: int):
    """
    - 严格保留原接口/调用点
    - 定向修复：
      * 预测（func == run_keras_inference_in_process）在高内存下 soft-skip（不抛 MemoryError）
      * 训练（func == run_training_in_process）高内存时清理+等待一次，再继续；仍高则抛 MemoryError
      * 在执行前检测 FD 水位线，超阈值重启进程池，避免 Errno 24
      * 显式处理 OSError(24)：重启进程池并返回 None（不抛给上层）
    """
    global _consecutive_failures, _pool_state, _fd_limits_logged

    initial_memory = psutil.Process().memory_info().rss / 1024 ** 2
    max_retries = 1

    if not _fd_limits_logged:
        logging.info(
            f"[execute] FD limits: soft={_effective_nofile_soft()}, "
            f"resource_soft={_RLIMIT_NOFILE_SOFT}, resource_hard={_RLIMIT_NOFILE_HARD}, "
            f"watermark={FD_WATERMARK}, hard={FD_HARD_LIMIT}, restart={FD_RESTART_LIMIT}"
        )
        _fd_limits_logged = True

    # 判别调用类型（不改调用点）
    func_name = getattr(func, "__name__", "")
    is_predict = (func_name == "run_keras_inference_in_process")
    is_train   = (func_name == "run_training_in_process")

    for attempt in range(max_retries + 1):
        try:
            # —— 高内存判定 ——
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > MEMORY_THRESHOLD_PERCENT:
                if is_predict:
                    logging.warning(f"[execute] 高内存 {memory_percent:.1f}% ，预测 soft-skip（返回 None）")
                    return None
                elif is_train:
                    logging.warning(f"[execute] 高内存 {memory_percent:.1f}% ，训练等待 {CLEANUP_WAIT_TIME}s 后重试一次")
                    force_cleanup_memory()
                    await asyncio.sleep(CLEANUP_WAIT_TIME)
                    # 再次检测
                    memory_percent2 = psutil.virtual_memory().percent
                    if memory_percent2 > MEMORY_THRESHOLD_PERCENT:
                        logging.error(f"任务异常返回系统内存不足")
                        raise MemoryError(f"系统内存不足: {memory_percent2:.1f}%")
                else:
                    logging.error(f"任务异常返回系统内存不足2")
                    # 默认行为与过去一致
                    raise MemoryError(f"系统内存不足: {memory_percent:.1f}%")

            # —— FD 水位线（Errno 24 防线） ——
            fdc = _num_open_fds()
            if fdc != -1 and fdc > FD_WATERMARK:
                logging.warning(
                    f"[execute] 打开文件句柄过多（{fdc}>{FD_WATERMARK}，硬线={FD_HARD_LIMIT}），"
                    f"尝试重启进程池+清理后重试"
                )
                shutdown_process_pool(force=True)
                kill_zombie_processes()
                force_cleanup_memory()
                await asyncio.sleep(1)
                fdc_after = _num_open_fds()
                logging.warning(f"[execute] FD 清理后：{fdc} -> {fdc_after}")
                # 仍超过硬安全线才真正跳过本次任务，避免把消费者任务直接打死
                if fdc_after != -1 and fdc_after >= FD_RESTART_LIMIT:
                    _request_process_restart(
                        f"FD 清理后仍接近耗尽({fdc_after}>={FD_RESTART_LIMIT}, hard={FD_HARD_LIMIT})"
                    )
                if fdc_after != -1 and fdc_after > FD_HARD_LIMIT:
                    _request_process_restart(
                        f"FD 清理后仍过高({fdc_after}>{FD_HARD_LIMIT})"
                    )
                # 否则继续向下走，正常创建进程池并执行任务
                logging.info(f"[execute] FD 已回落到 {fdc_after}，继续执行任务")

            pool = get_process_pool()
            if not is_process_pool_healthy(pool):
                logging.warning("[execute] 检测到进程池不健康，重新创建...")
                shutdown_process_pool(force=True)
                pool = get_process_pool()

            _pool_state = PoolState.BUSY
            loop = asyncio.get_running_loop()
            task = loop.run_in_executor(pool, func, *args)
            result = await asyncio.wait_for(task, timeout=timeout)
            _pool_state = PoolState.IDLE
            _consecutive_failures = 0
            return result

        except asyncio.TimeoutError:
            _consecutive_failures += 1
            _pool_state = PoolState.FAILED
            logging.error(f"任务超时 ({timeout}s)，第 {attempt + 1} 次尝试")
            if attempt < max_retries:
                shutdown_process_pool(force=True)
                await asyncio.sleep(CLEANUP_WAIT_TIME)
            else:
                # 与原语义一致：由上层决定是否跳过
                logging.error(f"任务异常返回")
                await asyncio.sleep(1)
                raise

        except Exception as e:
            logging.error(f"任务异常返回")
            _consecutive_failures += 1
            _pool_state = PoolState.FAILED
            el = str(e).lower()
            logging.error(f"[execute] Caught unhandled exception: {e}")
            logging.error(f"[execute] 捕获到未处理的异常: {e}", exc_info=True)
            # —— 显式处理 Errno 24 ——
            if isinstance(e, OSError) and getattr(e, "errno", None) == 24:
                _request_process_restart("触发 Errno 24（FD 耗尽）")

            # —— 清洗“池子意外终止”类报错，不向外抛原句 ——
            trigger_phrases = [
                "terminated abruptly while the future was running or pending",
                "brokenprocesspool",
                "a process in the process pool was terminated abruptly",
            ]
            if any(p in el for p in trigger_phrases):
                logging.error("[execute] 子进程异常退出，已自动重建进程池（任务跳过或重试）")
                shutdown_process_pool(force=True)
                kill_zombie_processes()
                if attempt < max_retries:
                    await asyncio.sleep(CLEANUP_WAIT_TIME)
                    continue
                return None

            # 其它与进程池相关的异常，重建后再试一次
            is_pool_error = any(k in el for k in ['process', 'pool', 'terminated', 'abruptly', 'broken'])
            if is_pool_error and attempt < max_retries:
                logging.info("[execute] 进程池错误，重新创建后重试")
                shutdown_process_pool(force=True)
                kill_zombie_processes()
                await asyncio.sleep(CLEANUP_WAIT_TIME)
                continue
            # 保持原语义：上抛
            logging.info(f"任务异常返回raise")
            raise
        finally:
            _pool_state = PoolState.IDLE
            current_memory = psutil.Process().memory_info().rss / 1024 ** 2
            memory_increase = current_memory - initial_memory
            if memory_increase > MAX_MEMORY_PER_TASK_MB:
                logging.warning(f"任务内存增长过多: {memory_increase:.2f}MB")
                force_cleanup_memory()

# ============================ 队列模型定义（保持不变） ============================
class TaskKind(str, Enum):
    TRAIN = "train"
    PREDICT = "predict"

@dataclass
class WorkItem:
    kind: TaskKind
    mode: str
    enqueued_at: float = field(default_factory=time.time)
    key: str = field(init=False)

    def __post_init__(self):
        self.key = f"{self.kind}:{self.mode}"

# =========================== 组合预测器主体（保持 3-3 语义） ===========================
class PortfolioPredictor:
    """基于单一工作队列的稳态执行器（3-3 逻辑不变，+读写分离与备份）"""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.log = logging.getLogger("Portfolio")
        self.loop = asyncio.get_running_loop()
        self.syms = cfg["general"]["symbols"]
        self.modes = {k: tuple(v) for k, v in cfg["modes"].items()}

        # 单实例 HTTP 客户端（与 3-3 一致）
        self.http = HTTPClient(cfg.get("api", {}).get("proxies", {}), 0)
        self.fetcher = DataFetcher(cfg, self.http)
        self.sentiment = Sentiment(cfg, self.http)

        # ---- Incremental Kline feature store (raw_kline/enhanced_kline/model_registry) ----
        self.kline_store_error = None
        try:
            # Keep the active store path explicit so a rebuilt candidate can be
            # reviewed, switched and rolled back without renaming either file.
            configured_store = (
                os.getenv("AI_BOT_KLINE_FEATURE_STORE_PATH", "").strip()
                or cfg["general"].get("kline_feature_store_path")
            )
            kfs_db = (
                Path(str(configured_store))
                if configured_store
                else Path(cfg["general"].get("db_dir", "./data")) / "kline_feature_store.sqlite3"
            )
            self.kline_store = KlineFeatureStore(kfs_db, cfg, self.fetcher)
        except Exception as exc:
            self.log.error(f"KlineFeatureStore 初始化失败: {exc}", exc_info=True)
            self.kline_store = None
            self.kline_store_error = f"{type(exc).__name__}: {exc}"

        # ---- OpenAI 兼容辅助预测器（默认禁用，配置后才生效，失败回退中性） ----
        self.llm_aux = OpenAIFormatSignalClient(cfg.get("llm_aux", {}))

        # ---- 在线学习校准器（“越用越聪明”） ----
        try:
            self.calibrator = OnlinePredictionCalibrator(cfg)
        except Exception as exc:
            self.log.warning(f"在线学习校准器初始化失败，已禁用: {exc}")
            self.calibrator = None

        # === 读写分离 + 备份（在不改变 3-3 对外 API 的前提下） ===
        self.root_dir = Path(cfg["general"]["model_dir"]); self.root_dir.mkdir(exist_ok=True)
        self.model_read_dir  = self.root_dir / "read"
        self.model_write_dir = self.root_dir / "write"
        self.backup_dir      = self.root_dir / "backup"
        self.model_read_dir.mkdir(parents=True, exist_ok=True)
        self.model_write_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        # 兼容 3-3：内部使用 self.model_dir 代表“读取目录”（预测侧）
        self.model_dir = self.model_read_dir

        # === 队列相关（保持不变） ===
        self._queue: "asyncio.Queue[WorkItem]" = asyncio.Queue(maxsize=200)
        self._pending_keys: Set[str] = set()   # 防抖：同 key 只允许一个待执行
        self._active_key: Opt[str] = None
        self._stop_event = asyncio.Event()
        self._consumer_task: Opt[asyncio.Task] = None
        self._watchdog_task: Opt[asyncio.Task] = None
        self._work_lock = asyncio.Lock()       # 双重保险，保证同进程内绝对串行
        self._last_progress_ts = time.time()
        self._current_item_started_at: Opt[float] = None
        self._current_item_desc: Opt[str] = None

        # 子进程自动回收
        self._setup_child_reaping()

        # 信号处理（优雅退出）
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    # ------------------------- 队列 API（保持不变） -------------------------

    def _touch_progress(self):
        self._last_progress_ts = time.time()

    def _on_consumer_done(self, task: asyncio.Task):
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            self.log.info("队列消费者已取消")
            return
        except Exception as e:
            exc = e

        if exc is None:
            # 正常停止（stop_worker/close 触发）不应当作异常
            if self._stop_event.is_set():
                self.log.info("队列消费者已正常停止")
            else:
                self.log.error("队列消费者意外退出（无异常对象）")
        else:
            self.log.error(f"队列消费者异常退出: {exc}", exc_info=exc)

    async def _worker_watchdog_loop(self):
        self.log.info("🩺 worker watchdog 已启动")
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(WORKER_WATCHDOG_INTERVAL)

                consumer = self._consumer_task
                if consumer is None or consumer.done():
                    # 若已被请求停止，不要再把消费者拉起来
                    if self._stop_event.is_set():
                        self.log.info("watchdog: stop_event 已设置，消费者保持停止状态")
                        break
                    self.log.warning("⚠️ 检测到消费者未运行，准备自动拉起")
                    self.start_worker()
                    continue

                if self._current_item_started_at and self._current_item_desc:
                    running_sec = time.time() - self._current_item_started_at
                    warn_threshold = int(
                        self.cfg.get("forecast", {}).get(
                            "mode_timeout_sec",
                            DEFAULT_MODE_TIMEOUT_PREDICT
                        )
                    )
                    warn_threshold = max(120, warn_threshold // 2)
                    if running_sec >= warn_threshold:
                        self.log.warning(
                            f"⚠️ 当前任务持续执行过久: {self._current_item_desc}, 已运行 {running_sec:.1f}s"
                        )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.error(f"watchdog 异常: {e}", exc_info=True)

        self.log.info("🩺 worker watchdog 已停止")

    def start_worker(self):
        """启动单一消费者（若已启动则忽略），并附带 watchdog 自愈"""
        if self._consumer_task and not self._consumer_task.done():
            if self._watchdog_task is None or self._watchdog_task.done():
                self._watchdog_task = asyncio.create_task(
                    self._worker_watchdog_loop(),
                    name="portfolio-watchdog",
                )
            return

        self._stop_event.clear()
        self._consumer_task = asyncio.create_task(
            self._consumer_loop(),
            name="portfolio-consumer",
        )
        self._consumer_task.add_done_callback(self._on_consumer_done)

        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(
                self._worker_watchdog_loop(),
                name="portfolio-watchdog",
            )

        self.log.info("📦 队列消费者已启动")

    async def stop_worker(self):
        """停止消费者并等待队列清空"""
        self._stop_event.set()

        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            finally:
                self._watchdog_task = None

        if not self._consumer_task:
            return

        await asyncio.sleep(1)
        try:
            await asyncio.wait_for(self._consumer_task, timeout=60)
        except asyncio.TimeoutError:
            self._consumer_task.cancel()
            self.log.warning("队列消费者停止超时，已取消任务")
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        finally:
            self._consumer_task = None
            self._active_key = None
            self._current_item_started_at = None
            self._current_item_desc = None
            self._pending_keys.clear()

    def enqueue_train(self, mode: str):
        """将训练任务放入队列（防抖，合并重复模式）"""
        global MAX_MEMORY_PER_TASK_MB
        MAX_MEMORY_PER_TASK_MB = int(os.environ.get("MAX_MEMORY_PER_TASK_MB", "4000"))
        self._enqueue(TaskKind.TRAIN, mode)

    def enqueue_predict(self, mode: str):
        """将预测任务放入队列（防抖，合并重复模式）"""
        self._enqueue(TaskKind.PREDICT, mode)

    def _enqueue(self, kind: TaskKind, mode: str):
        item = WorkItem(kind=kind, mode=mode)
        if item.key in self._pending_keys or item.key == self._active_key:
            # 已在队列或正在执行，直接合并（丢弃重复）
            self.log.info(f"🕒 合并重复任务（{item.key}），当前队列长度={self._queue.qsize()}")
            return
        try:
            self._queue.put_nowait(item)
            self._pending_keys.add(item.key)
            self._touch_progress()
            self.log.info(f"📥 入队: {item.key} (queue={self._queue.qsize()})")
        except asyncio.QueueFull:
            # 队列满了时，丢弃最旧的同类任务：保证系统继续前进
            self.log.warning(f"队列已满，放弃入队: {item.key}")

    async def _consumer_loop(self):
        """单消费者循环：严格串行执行，任何 BaseException 都会记录，避免任务静默消失"""
        self.log.info("消费者循环开始运行（串行执行任务）")
        self._touch_progress()

        while not self._stop_event.is_set():
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # 队列空：若已被请求停止，直接干净退出，避免外部把它当成"意外退出"
                if self._stop_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                raise

            self._active_key = item.key
            self._pending_keys.discard(item.key)
            self._current_item_started_at = time.time()
            self._current_item_desc = item.key
            self._touch_progress()
            t0 = time.time()

            try:
                async with self._work_lock:
                    self.log.info(f"🚀 执行任务: {item.key}，队列剩余={self._queue.qsize()}")

                    if item.kind == TaskKind.TRAIN:
                        timeout = int(
                            self.cfg.get("training", {}).get(
                                "mode_timeout_sec",
                                DEFAULT_MODE_TIMEOUT_TRAIN,
                            )
                        )
                        await asyncio.wait_for(self.train_mode(item.mode), timeout=timeout)
                    else:
                        timeout = int(
                            self.cfg.get("forecast", {}).get(
                                "mode_timeout_sec",
                                DEFAULT_MODE_TIMEOUT_PREDICT,
                            )
                        )
                        await asyncio.wait_for(self.predict_modes(item.mode), timeout=timeout)

            except asyncio.TimeoutError:
                self.log.error(f"执行任务 {item.key} 超时，已跳过本轮")
            except asyncio.CancelledError:
                raise
            except BaseException as e:
                self.log.error(f"执行任务 {item.key} 发生致命异常: {e}", exc_info=True)
            finally:
                self._active_key = None
                self._current_item_started_at = None
                self._current_item_desc = None
                try:
                    self._queue.task_done()
                except ValueError as e:
                    self.log.warning(f"queue.task_done 重复调用或状态异常: {e}")
                force_cleanup_memory()
                kill_zombie_processes()
                self._touch_progress()
                self.log.info(f"✅ 完成任务: {item.key}，耗时 {time.time()-t0:.1f}s")

        self.log.info("消费者循环已停止")

    # ---------------------- 生命周期 & 清理（保持不变） ----------------------
    async def close(self):
        """优雅关闭所有异步资源（先停队列，再关网络）"""
        try:
            await self.stop_worker()
        except Exception:
            pass
        try:
            await self.fetcher.close()
        except Exception:
            pass
        try:
            await self.http.aclose()  # 单实例 client，全流程只此一次关闭
        except Exception:
            pass
        # 进程池 & 内存
        shutdown_process_pool(force=True)
        kill_zombie_processes()
        force_cleanup_memory()

    def _setup_child_reaping(self):
        def reap_children(signum, frame):
            while True:
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                    self.log.debug(f"自动回收子进程 PID {pid}")
                except OSError:
                    break
        try:
            signal.signal(signal.SIGCHLD, reap_children)
            self.log.info("已设置子进程自动回收机制")
        except (OSError, ValueError) as e:
            self.log.warning(f"无法设置子进程回收机制: {e}")

    def _signal_handler(self, signum, frame):
        self.log.info(f"接收到信号 {signum}，正在清理资源...")
        # 尽量同步清理可同步的资源；异步资源由上层 await close()
        shutdown_process_pool(force=True)
        kill_zombie_processes()
        force_cleanup_memory()
        try:
            # 在事件循环中安排真正的异步关闭
            asyncio.get_running_loop().create_task(self.close())
        except RuntimeError:
            pass
        sys.exit(0)

    def cleanup(self):
        """兼容旧接口"""
        self.log.info("开始清理资源...")
        shutdown_process_pool(force=True)
        kill_zombie_processes()
        force_cleanup_memory()
        try:
            asyncio.get_running_loop().create_task(self.close())
        except RuntimeError:
            pass
        self.log.info("资源清理完成")

    # --------------------- 模式参数与健康检查（保持不变） ---------------------
    def _get_mode_config(self, mode: str):
        config_tuple = self.modes.get(mode)
        if not config_tuple:
            raise ValueError(f"未找到模式 '{mode}' 的配置。")
        tf_code, limit, window = config_tuple
        return tf_code, limit, window

    async def _check_system_health(self):
        memory_percent = psutil.virtual_memory().percent
        process_memory = psutil.Process().memory_info().rss / 1024 ** 2
        self.log.debug(f"系统内存: {memory_percent:.1f}%, 进程内存: {process_memory:.1f}MB")
        if memory_percent >= MEMORY_THRESHOLD_PERCENT:
            self.log.warning(f"系统内存使用率过高 ({memory_percent:.1f}%)，本轮任务跳过，避免消费者直接退出")
            return False
        return True

    async def _process_symbol_with_smart_retry(self, func, sym: str, mode: str):
        max_retries = 2
        base_delay = 3
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    if not await self._check_system_health():
                        self.log.error(f"[{sym}-{mode}] 系统资源不足，跳过重试")
                        return None
                    delay = base_delay * (2 ** (attempt - 1))
                    self.log.info(f"[{sym}-{mode}] 第 {attempt + 1} 次重试，等待 {delay}s")
                    force_cleanup_memory()
                    await asyncio.sleep(delay)
                return await func(sym, mode)
            except Exception as e:
                self.log.error(f"[{sym}-{mode}] 尝试 {attempt + 1} 失败: {e}")
                is_memory_error = any(keyword in str(e).lower() for keyword in [
                    'memory', 'oom', 'out of memory', 'allocation'
                ])
                is_process_error = any(keyword in str(e).lower() for keyword in [
                    'process', 'pool', 'terminated', 'abruptly'
                ])
                if is_memory_error:
                    self.log.error(f"[{sym}-{mode}] 内存不足错误，跳过重试")
                    break
                if attempt >= max_retries:
                    self.log.error(f"[{sym}-{mode}] 所有重试都失败，跳过")
                    break
                if is_process_error:
                    shutdown_process_pool(force=True)
                    kill_zombie_processes()
        return None

    # --------------------------- 读写分离 / 备份 / 自愈 ---------------------------
    def _promote_with_backup(self, src: Path, dst: Path):
        """write -> read 原子推广，并在 backup/ 生成同名备份"""
        try:
            if not _is_valid_file(src):
                self.log.warning(f"[promote] 缺少产物，未推广: {src.name}")
                return False

            # 不给 read 上锁，以保持 3-3 的速度语义；采用原子替换避免读阻塞
            _atomic_copy(src, dst, self.log)
            self.log.info(f"[promote] write → read (atomic): {dst.name}")

            # 备份到 backup/
            backup_target = self.backup_dir / dst.name
            _atomic_copy(dst, backup_target, self.log)
            self.log.info(f"[backup] 已备份到 backup/{dst.name}")
            return True
        except Exception as e:
            self.log.warning(f"[promote] 推广 {src.name} 失败: {e}")
            return False

    def _ensure_artifact_for_predict(self, sym: str, tf_code: str) -> bool:
        """预测前确保 read/ 有需要的 .keras 与 _scaler.pkl；缺或损坏则从 backup/ 或 write/ 修复"""
        ok = True
        for name in (f"{sym}_{tf_code}.keras", f"{sym}_{tf_code}_scaler.pkl"):
            read_p = self.model_read_dir / name
            if _is_valid_file(read_p):
                continue
            # 候选来源优先：backup -> write
            candidates = [self.backup_dir / name, self.model_write_dir / name]
            src = next((c for c in candidates if _is_valid_file(c)), None)
            if not src:
                self.log.error(f"[infer-ensure] 缺少 {name}，无法预测（read/ 与 backup/write 均不存在或损坏）")
                ok = False
                continue
            try:
                _atomic_copy(src, read_p, self.log)
                self.log.warning(f"[infer-ensure] read 缺失/损坏，已从 {src.parent.name} 修复 {name}")
            except Exception as e:
                self.log.error(f"[infer-ensure] 修复 {name} 失败: {e}")
                ok = False
        return ok

    # ----------------------------- 训练（保持语义，仅改写入目录+推广） -----------------------------
    async def _train_single_symbol(self, sym: str, mode: str):
        data_preparer = None
        prepared_data = None
        try:
            epochs = self.cfg["training"]["epochs"].get(mode, 20)
            batch = self.cfg["training"]["batch_size"]
            tf_code, limit, window = self._get_mode_config(mode)
            self.log.info(f"[{sym}-{mode}] 准备训练数据...")
            mode_spec = None
            built_dataset = None
            store_update = None
            require_versioned_store = bool(
                (self.cfg.get("training") or {}).get("require_versioned_feature_store", True)
            )
            if self.kline_store is None and require_versioned_store:
                self.log.error(
                    f"[{sym}-{mode}] 禁止训练：版本化特征库不可用: {self.kline_store_error}"
                )
                return None
            if self.kline_store is not None:
                try:
                    mode_spec = self.kline_store.spec_for(sym, mode)
                    store_update = await self.kline_store.update_for_mode(mode_spec)
                    built_dataset = self.kline_store.build_mode_dataset(mode_spec)
                    lstm_should, lstm_reason = self.kline_store.should_train(
                        "lstm", mode_spec, built_dataset.signature,
                        str(self.model_read_dir / f"{sym}_{tf_code}.keras"),
                        int(store_update.base_new_rows),
                    )
                    if not lstm_should:
                        self.kline_store.record_model("lstm", mode_spec, built_dataset.signature, str(self.model_read_dir / f"{sym}_{tf_code}.keras"), "skipped", lstm_reason)
                        self.log.info(f"[{sym}-{mode}] LSTM跳过训练: {lstm_reason}")
                        return True
                except FeatureStoreIntegrityError as exc:
                    self.log.error(
                        f"[{sym}-{mode}] 禁止训练：版本化特征证据完整性失败: {exc}",
                        exc_info=True,
                    )
                    return None
                except Exception as exc:
                    if require_versioned_store:
                        self.log.error(
                            f"[{sym}-{mode}] 禁止训练：版本化特征准备失败: {exc}",
                            exc_info=True,
                        )
                        return None
                    self.log.warning(f"[{sym}-{mode}] KlineFeatureStore 准备失败，回退旧训练路径: {exc}", exc_info=True)
                    mode_spec = None; built_dataset = None; store_update = None
            # === 训练输出目录使用 write/；有 built_dataset 时不再全量拉取/现场重算增强特征 ===
            data_preparer = TrainerDataPreparer(
                sym, tf_code, (limit, window), self.model_write_dir,
                self.fetcher, self.sentiment,
                cfg=self.cfg, mode=mode, results_dir=RESULTS_DIR,
                feature_store=self.kline_store, mode_spec=mode_spec, built_dataset=built_dataset,
            )
            prepared_data = await data_preparer.prepare_data_for_process(batch, epochs)
            if not prepared_data:
                self.log.warning(f"[{sym}-{mode}] 数据准备失败，跳过训练")
                return None
            self.log.info(f"[{sym}-{mode}] 开始训练...")
            # ---- 训练更长超时：默认 2 小时，可配置 training.task_timeout_sec ----
            train_timeout = int(self.cfg.get("training", {}).get("task_timeout_sec", DEFAULT_TASK_TIMEOUT_TRAIN))
            result = await execute_with_memory_limit(
                run_training_in_process, prepared_data, timeout=train_timeout
            )
            if not result:
                self.log.warning(f"[{sym}-{mode}] 训练未产生有效结果")
                return None

            # === 训练完成后推广到 read/ 并备份（保证可自愈） ===
            ok1 = self._promote_with_backup(
                self.model_write_dir / f"{sym}_{tf_code}.keras",
                self.model_read_dir  / f"{sym}_{tf_code}.keras"
            )
            ok2 = self._promote_with_backup(
                self.model_write_dir / f"{sym}_{tf_code}_scaler.pkl",
                self.model_read_dir  / f"{sym}_{tf_code}_scaler.pkl"
            )
            if not (ok1 and ok2):
                self.log.warning(f"[{sym}-{mode}] 推广或备份存在失败项")
            self.log.info(f"[{sym}-{mode}] 训练完成并推广备份")

            # Brain 模型：基于本地 enhanced_kline / 历史K线训练方向模型；避免再次全量拉取。
            try:
                if built_dataset is not None:
                    brain_df = built_dataset.df.copy()
                else:
                    brain_df = await data_preparer._feature_df()
                brain_meta = train_brain_from_df(brain_df, sym, tf_code, mode, self.cfg)
                self.log.info(f"[{sym}-{mode}] Brain训练状态: {brain_meta.get('status')} signature={brain_meta.get('data_signature')}")
                if isinstance(result, dict):
                    result["brain_training"] = brain_meta
            except Exception as exc:
                self.log.warning(f"[{sym}-{mode}] Brain训练失败: {exc}", exc_info=True)
                if isinstance(result, dict):
                    result["brain_training"] = {"status": "failed", "error": str(exc)}

            # 训练元数据：result 是 run_training_in_process 返回的 meta
            try:
                if isinstance(result, dict):
                    rm.save_training_metadata(sym, mode, result)
            except Exception as exc:
                self.log.debug(f"训练元数据保存失败: {exc}")
            try:
                if self.kline_store is not None and mode_spec is not None and built_dataset is not None:
                    self.kline_store.record_model("lstm", mode_spec, built_dataset.signature, str(self.model_read_dir / f"{sym}_{tf_code}.keras"), "trained", "trained")
                    self.kline_store.record_model("brain", mode_spec, built_dataset.signature, str(__import__('core.brain_model', fromlist=['brain_paths']).brain_paths(sym, mode, self.cfg)[0]), "trained", str((result or {}).get("brain_training", {}).get("status", "trained")))
            except Exception as exc:
                self.log.debug(f"model_registry 记录失败: {exc}")
            return True
        finally:
            try:
                del data_preparer; del prepared_data
            except Exception:
                pass
            force_cleanup_memory()

    async def train_mode(self, mode: str):
        self.log.info(f"开始模式 '{mode}' 的训练任务...")
        successful_count = 0
        failed_count = 0
        try:
            for i, sym in enumerate(self.syms):
                try:
                    self.log.info(f"处理交易对 {i + 1}/{len(self.syms)}: {sym}")
                    if not await self._check_system_health():
                        self.log.warning(f"系统资源不足，暂停处理 {sym}")
                        failed_count += 1
                        force_cleanup_memory()
                        kill_zombie_processes()
                        await asyncio.sleep(5)
                        continue
                    result = await self._process_symbol_with_smart_retry(
                        self._train_single_symbol, sym, mode
                    )
                    if result is not None:
                        successful_count += 1
                    else:
                        failed_count += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    failed_count += 1
                    self.log.error(f"[{sym}-{mode}] 训练阶段异常: {e}", exc_info=True)
                finally:
                    force_cleanup_memory()
                    await asyncio.sleep(2)
        except Exception as e:
            self.log.error(f"模式 '{mode}' 训练过程中发生严重错误: {e}", exc_info=True)
        finally:
            force_cleanup_memory()
            kill_zombie_processes()
            self.log.info(f"模式 '{mode}' 训练完成 - 成功: {successful_count}, 失败: {failed_count}")

    # ----------------------------- 在线学习结算（基于本地K线缓存） -----------------------------
    def _actual_return_from_cache(self, symbol: str, timeframe: str, last_price: float, settle_at: int):
        """同步读取 data/{SYMBOL}.sqlite 中到期后的第一根K线 close，回填真实收益。"""
        try:
            db_path = Path(self.cfg["general"]["db_dir"]) / f"{symbol}.sqlite"
            table = f"k_{timeframe}"
            if not db_path.exists() or not last_price:
                return None
            con = sqlite3.connect(str(db_path))
            rows = con.execute(f"SELECT ts, close FROM {table} ORDER BY ts ASC").fetchall()
            con.close()
            if not rows:
                return None
            target = float(settle_at)
            chosen = None
            for ts, close in rows:
                try:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    epoch = dt.timestamp()
                except Exception:
                    continue
                if epoch >= target:
                    chosen = float(close)
                    break
            if chosen is None:
                return None
            return (chosen - float(last_price)) / float(last_price)
        except Exception as exc:
            self.log.debug(f"在线学习结算读取真实收益失败: {symbol}-{timeframe}: {exc}")
            return None

    # ----------------------------- 预测（保持语义，仅加自愈） -----------------------------
    async def _predict_single_symbol(self, sym: str, mode: str):
        data_preparer = None
        prepared_data = None
        try:
            tf_code, limit, window = self._get_mode_config(mode)
            self.log.info(f"[{sym}-{mode}] 准备预测数据...")

            # === 自愈：预测前确保 read/ 有可用文件（缺/损坏则从 backup 或 write 修复） ===
            if not self._ensure_artifact_for_predict(sym, tf_code):
                self.log.warning(f"[{sym}-{mode}] 必要文件缺失或损坏，跳过预测")
                return None

            data_preparer = InferencerDataPreparer(
                sym, tf_code, (limit, window), self.model_read_dir,
                self.fetcher, self.sentiment,
                cfg=self.cfg, mode=mode,
                llm_aux=self.llm_aux, calibrator=self.calibrator,
            )
            prepare_timeout = int(self.cfg.get("forecast", {}).get("prepare_timeout_sec", 120))
            try:
                prepared_data = await asyncio.wait_for(
                    data_preparer.prepare_data_for_process(),
                    timeout=prepare_timeout,
                )
            except asyncio.TimeoutError:
                self.log.error(f"[{sym}-{mode}] 数据准备超时（{prepare_timeout}s），跳过预测")
                return None
            if not prepared_data:
                self.log.warning(f"[{sym}-{mode}] 数据准备失败，跳过预测")
                return None
            self.log.info(f"[{sym}-{mode}] 开始预测...")
            # ---- 预测短超时：默认 5 分钟，可配置 forecast.task_timeout_sec ----
            predict_timeout = int(self.cfg.get("forecast", {}).get("task_timeout_sec", DEFAULT_TASK_TIMEOUT_PREDICT))
            result = await execute_with_memory_limit(
                run_keras_inference_in_process, prepared_data, timeout=predict_timeout
            )
            if result:
                # Persist the slow external panel as shadow evidence only.  It
                # is intentionally not passed into the model or direction
                # fusion until PIT/OOS ablation approves a versioned contract.
                try:
                    result["external_panel_context"] = self.fetcher.get_external_panel_context()
                except Exception as external_exc:
                    result["external_panel_context"] = {
                        "status": "outage",
                        "source": "trad_data_service.canonical_panel",
                        "data": None,
                        "warnings": [],
                        "error": f"{type(external_exc).__name__}: {external_exc}",
                    }
                # 在线学习记录预测，便于后续回填学习
                try:
                    if self.calibrator is not None:
                        try:
                            settled = self.calibrator.settle_due(self._actual_return_from_cache)
                            if settled:
                                self.log.info(f"在线学习已结算 {settled} 条到期预测")
                        except Exception as settle_exc:
                            self.log.debug(f"在线学习结算失败: {settle_exc}")
                        try:
                            calibration = self.calibrator.calibrate(
                                sym,
                                tf_code,
                                mode,
                                float(result.get("predicted_return") or 0.0),
                                float(result.get("last") or 0.0),
                            )
                            result["calibrated_predicted_return"] = calibration.get(
                                "calibrated_predicted_return"
                            )
                            result["calibrated_return"] = calibration.get(
                                "calibrated_predicted_return"
                            )
                            result["calibrated_direction"] = calibration.get(
                                "calibrated_trend"
                            )
                            result["direction_confidence"] = calibration.get(
                                "direction_confidence"
                            )
                            result["calibration_status"] = calibration.get(
                                "calibration_status", "unknown"
                            )
                            result["online_learning"] = calibration.get("online_learning")
                        except Exception as calibration_exc:
                            result["calibration_status"] = "error"
                            self.log.debug(
                                f"在线学习校准失败，结果禁止出票: {calibration_exc}"
                            )
                        horizon_seconds_map = {
                            "scalping": 180, "mid_short": 900,
                            "trend": 7200, "trend_swing": 14400, "swing": 86400,
                        }
                        horizon = int(self.cfg.get("online_learning", {}).get("min_horizon_seconds", 60))
                        horizon = max(horizon, horizon_seconds_map.get(mode, 600))
                        # 旧位置参数保持兼容，新增的关键字元数据全部为可选项，
                        # OnlinePredictionCalibrator.record 会在列存在时落库。
                        _pred_dir = (
                            result.get("trade_direction")
                            or result.get("calibrated_trend")
                            or result.get("trend")
                        )
                        _conf = result.get("direction_confidence")
                        if _conf is None:
                            _conf = result.get("confidence")
                        _model_version = result.get("model_version")
                        _target_raw_return = result.get("target_raw_return")
                        _leverage = result.get("target_leverage")
                        _current_price = result.get("current_price")
                        _kline_last_price = result.get("kline_last_price")
                        _feature_snapshot_hash = result.get("feature_snapshot_hash")
                        self.calibrator.record(
                            sym, tf_code, mode,
                            float(result.get("predicted_return") or 0.0),
                            float(result.get("last") or 0.0),
                            horizon,
                            float(result.get("raw_predicted_return") or 0.0),
                            predicted_direction=_pred_dir,
                            confidence=(None if _conf is None else float(_conf)),
                            model_version=_model_version,
                            target_raw_return=(
                                None if _target_raw_return is None else float(_target_raw_return)
                            ),
                            leverage=(None if _leverage is None else float(_leverage)),
                            current_price=(
                                None if _current_price is None else float(_current_price)
                            ),
                            kline_last_price=(
                                None if _kline_last_price is None else float(_kline_last_price)
                            ),
                            feature_snapshot_hash=_feature_snapshot_hash,
                        )
                        # 写入一次后顺手导出评估摘要（若实现存在），失败不影响主链路。
                        _export_fn = getattr(self.calibrator, "export_evaluation_summary", None)
                        if callable(_export_fn):
                            try:
                                _export_fn()
                            except Exception as _export_exc:
                                self.log.debug(f"在线学习评估摘要导出失败: {_export_exc}")
                except Exception as exc:
                    self.log.debug(f"在线学习记录失败: {exc}")
                await rm.save_result(sym, mode, result)
                self.log.info(f"[{sym}-{mode}] 预测结果已保存")
                return result
            else:
                self.log.warning(f"[{sym}-{mode}] 未返回有效预测结果")
                return None
        finally:
            try:
                del data_preparer; del prepared_data
            except Exception:
                pass
            force_cleanup_memory()

    async def predict_modes(self, mode: str):
        self.log.info(f"开始模式 '{mode}' 的预测任务...")
        successful_count = 0
        failed_count = 0
        try:
            for i, sym in enumerate(self.syms):
                try:
                    self.log.info(f"处理交易对 {i + 1}/{len(self.syms)}: {sym}")
                    if not await self._check_system_health():
                        self.log.warning(f"系统资源不足，暂停处理 {sym}")
                        failed_count += 1
                        force_cleanup_memory()
                        kill_zombie_processes()
                        await asyncio.sleep(5)
                        continue
                    result = await self._process_symbol_with_smart_retry(
                        self._predict_single_symbol, sym, mode
                    )
                    if result is not None:
                        successful_count += 1
                    else:
                        failed_count += 1
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    failed_count += 1
                    self.log.error(f"[{sym}-{mode}] 预测阶段异常: {e}", exc_info=True)
                finally:
                    force_cleanup_memory()
                    await asyncio.sleep(1)  # 预测更快，稍微降点间隔
        except Exception as e:
            self.log.error(f"模式 '{mode}' 预测过程中发生严重错误: {e}", exc_info=True)
        finally:
            force_cleanup_memory()
            kill_zombie_processes()
            self.log.info(f"模式 '{mode}' 预测完成 - 成功: {successful_count}, 失败: {failed_count}")

    def __del__(self):
        try:
            # 避免在解释器关停阶段 await，做最小清理
            shutdown_process_pool(force=True)
        except Exception:
            pass

# 模块级清理（保持不变）
import atexit
def cleanup_module():
    shutdown_process_pool(force=True)
    kill_zombie_processes()
    force_cleanup_memory()
atexit.register(cleanup_module)
