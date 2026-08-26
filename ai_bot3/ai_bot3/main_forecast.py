import asyncio
import logging
import os
import signal
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
REPOSITORY_ROOT = PROJECT_ROOT.parents[1]
for candidate in (REPOSITORY_ROOT, PROJECT_ROOT):
    text = str(candidate)
    if text not in sys.path:
        sys.path.insert(0, text)

from core.service_runtime import load_predictor_runtime

_RUNTIME = load_predictor_runtime()


def _activate_runtime_data_root() -> Path:
    raw = os.environ.get("PREDICTOR_DATA_DIR", "").strip()
    if _RUNTIME.app_environment.value == "production" and not raw:
        raise RuntimeError("production predictor requires PREDICTOR_DATA_DIR")
    root = Path(raw).expanduser() if raw else PROJECT_ROOT
    if _RUNTIME.app_environment.value == "production" and not root.is_absolute():
        raise RuntimeError("production PREDICTOR_DATA_DIR must be absolute")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    os.chdir(root)
    return root


RUNTIME_DATA_ROOT = _activate_runtime_data_root()

from core.config_loader import load_config
from core.portfolio3_3_fixed import PortfolioPredictor
from core.scheduler import Scheduler, Job

logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def amain():
    cfg = load_config(str(PROJECT_ROOT / "config.yml"))
    portfolio = PortfolioPredictor(cfg)
    # 启动单消费者
    portfolio.start_worker()

    sched = Scheduler()

    # 仅预测：按 forecast.schedule 把任务入队（防抖合并）
    modes = cfg["forecast"]["schedule"]
    for mode, cron_expression in modes.items():
        async def task(m=mode, cron=cron_expression):
            logging.info(f"⏰ 触发预测：{m} - {cron}")
            portfolio.enqueue_predict(m)
        sched.add(Job(name=f"predict:{mode}", cron=cron_expression, coro_fn=task))

    # 优雅退出
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, sched.stop)

    await sched.run_forever()

    # 退出前清理
    await portfolio.close()


if __name__ == "__main__":
    asyncio.run(amain())
