#!/bin/bash

# =========================================================================
# 配置代理和日志目录
# =========================================================================
# 定义 SOCKS 代理地址和端口
# 请确保你的 autossh SOCKS 代理隧道已经运行在 127.0.0.1:1080
#SOCKS_PROXY_ADDRESS="socks5://127.0.0.1:1080"

# 定义日志目录
LOG_DIR="logs"
PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python3}"
if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

# 确保 logs 目录存在（提前创建，singleton lock 也可能落在此目录）
mkdir -p "${LOG_DIR}"

# =========================================================================
# 项目级单例锁：防止重复启动产生 main_train / main_forecast 重复父进程
# =========================================================================
# 优先使用 /tmp 路径，便于跨用户共享；若不可写则退回 logs/run_v3.lock。
RUN_V3_LOCK_FILE="${RUN_V3_LOCK_FILE:-/tmp/ai_bot3_run_v3.lock}"
if ! ( : > "$RUN_V3_LOCK_FILE" ) 2>/dev/null; then
  RUN_V3_LOCK_FILE="${LOG_DIR}/run_v3.lock"
  : > "$RUN_V3_LOCK_FILE" 2>/dev/null || true
fi
# 打开锁文件并保持 FD 200 在整个脚本生命周期内有效（包括后台 supervise 子 shell 通常会继承）。
exec 200>"$RUN_V3_LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
  if ! flock -n 200; then
    echo "[run_v3.sh] 另一个 run_v3.sh 实例正在运行 (lock=$RUN_V3_LOCK_FILE)，本次启动被跳过。"
    exit 0
  fi
else
  echo "[run_v3.sh] 警告: 系统未安装 flock，无法强制单例。继续启动。"
fi

# =========================================================================
# CPU 线程上限（防止 numpy/BLAS/MKL/TensorFlow 各自抢满所有核心）
# 必须在任何 python 子进程启动之前 export，以便它们继承。
# =========================================================================
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-2}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export TF_NUM_INTRAOP_THREADS="${TF_NUM_INTRAOP_THREADS:-2}"
export TF_NUM_INTEROP_THREADS="${TF_NUM_INTEROP_THREADS:-1}"
export TF_FORCE_GPU_ALLOW_GROWTH="${TF_FORCE_GPU_ALLOW_GROWTH:-true}"

# Ensure project root is importable when executing api/api_server.py by path.
export PYTHONPATH="$(pwd):${PYTHONPATH}"

# 训练/预测 TensorFlow/多进程稳定态会占用较多文件句柄；默认 1024 会导致任务被 FD guard 跳过。
ulimit -n 65535 2>/dev/null || true
export FD_WATERMARK="${FD_WATERMARK:-60000}"
export FD_HARD_LIMIT="${FD_HARD_LIMIT:-65000}"
export FD_RESTART_LIMIT="${FD_RESTART_LIMIT:-64500}"

# CUDA / cuDNN shared library paths from pip-installed NVIDIA wheels.
SP="$PWD/.venv/lib/python3.12/site-packages"
CUDA_LD="$SP/nvidia/cuda_runtime/lib:$SP/nvidia/cudnn/lib:$SP/nvidia/cublas/lib:$SP/nvidia/cufft/lib:$SP/nvidia/curand/lib:$SP/nvidia/cusolver/lib:$SP/nvidia/cusparse/lib:$SP/nvidia/nccl/lib:$SP/nvidia/nvjitlink/lib:$SP/nvidia/cuda_cupti/lib:$SP/nvidia/cuda_nvrtc/lib"
export LD_LIBRARY_PATH="$CUDA_LD:${LD_LIBRARY_PATH:-}"

# =========================================================================
# 启动各项服务，并强制它们走代理
# =========================================================================

# 长期运行自愈：训练/预测进程异常退出或主动 self-heal 退出后自动拉起。
supervise_service() {
  local name="$1"
  local script="$2"
  local log_file="$3"
  (
    while true; do
      echo "[$(date '+%F %T')] supervisor: start ${name}" >> "${log_file}"
      "$PYTHON_BIN" "$script" >> "${log_file}" 2>&1
      code=$?
      echo "[$(date '+%F %T')] supervisor: ${name} exited code=${code}, restart in 5s" >> "${log_file}"
      sleep 5
    done
  ) &
}

# 启动爆仓图爬虫 (liqmap_fetcher.py)
echo "启动爆仓图爬虫 (liqmap_fetcher.py) ..."
# 在nohup命令前设置环境变量，确保其对该进程生效
supervise_service "liqmap_fetcher" "liqmap_fetcher.py" "${LOG_DIR}/liqmap_fetcher.log"
sleep 1 # 短暂暂停，让进程有时间启动

# 启动 FastAPI 服务 (api/api_server.py)
echo "启动 API Server (api/api_server.py) ..."
nohup "$PYTHON_BIN" api/api_server.py > "${LOG_DIR}/api_server.log" 2>&1 &
sleep 1 # 短暂暂停

# 启动 Web Server (api/web_server.py)
echo "启动 Web Server (api/web_server.py) ..."
nohup "$PYTHON_BIN" api/web_server.py > "${LOG_DIR}/web_server.log" 2>&1 &

echo "启动 训练 服务 (main_train.py)  ..."
supervise_service "main_train" "main_train.py" "${LOG_DIR}/main_train.log"
sleep 1 # 短暂暂停

echo "启动 预测 服务 (main_forecast.py)  ..."
supervise_service "main_forecast" "main_forecast.py" "${LOG_DIR}/main_forecast.log"

# 注意：旧的 main.py / main_supervisor.py 已不在活跃链路中。
# 当前活跃链路只有 liqmap_fetcher.py + api/api_server.py + api/web_server.py
# + main_train.py + main_forecast.py，请勿恢复 main.py 入口。

# =========================================================================
# 最终状态和信息
# =========================================================================
echo -e "\n所有服务已尝试启动，并已配置为通过代理访问网络。"
echo "请检查以下日志文件以确认状态和代理是否生效："
echo "- ${LOG_DIR}/liqmap_fetcher.log"
echo "- ${LOG_DIR}/api_server.log"
echo "- ${LOG_DIR}/web_server.log"
echo "- ${LOG_DIR}/main_train.log"
echo "- ${LOG_DIR}/main_forecast.log"

# 展示当前运行的 Python 进程（过滤掉 grep 自身）
echo -e "\n当前 Python 相关的后台进程："
ps -ef | grep python | grep -v grep