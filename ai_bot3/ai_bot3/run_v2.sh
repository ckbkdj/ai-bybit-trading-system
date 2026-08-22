#!/bin/bash

# =========================================================================
# 配置代理和日志目录
# =========================================================================
# 定义 SOCKS 代理地址和端口
# 请确保你的 autossh SOCKS 代理隧道已经运行在 127.0.0.1:1080
SOCKS_PROXY_ADDRESS="socks5://127.0.0.1:1080"

# 定义日志目录
LOG_DIR="logs"

# 确保 logs 目录存在，如果不存在则创建它
mkdir -p "${LOG_DIR}"

# =========================================================================
# 启动各项服务，并强制它们走代理
# =========================================================================

# 启动爆仓图爬虫 (liqmap_fetcher.py)
echo "启动爆仓图爬虫 (liqmap_fetcher.py) 并强制走代理..."
# 在nohup命令前设置环境变量，确保其对该进程生效
nohup env ALL_PROXY="${SOCKS_PROXY_ADDRESS}" python3 liqmap_fetcher.py > "${LOG_DIR}/liqmap_fetcher.log" 2>&1 &
sleep 1 # 短暂暂停，让进程有时间启动

# 启动 FastAPI 服务 (api/api_server.py)
echo "启动 API Server (api/api_server.py) 并强制走代理..."
nohup env ALL_PROXY="${SOCKS_PROXY_ADDRESS}" python3 api/api_server.py > "${LOG_DIR}/api_server.log" 2>&1 &
sleep 1 # 短暂暂停

# 启动 Web Server (api/web_server.py)
echo "启动 Web Server (api/web_server.py) 不走代理..."
nohup  python3 api/web_server.py > "${LOG_DIR}/web_server.log" 2>&1 &

echo "启动 训练 服务 (main_train.py) with Supervisor 并强制走代理..."
nohup env ALL_PROXY="${SOCKS_PROXY_ADDRESS}" python3 main_train.py > "${LOG_DIR}/main_train.log" 2>&1 &
sleep 320 # 短暂暂停

echo "启动 训练 服务 (main_forecast.py) with Supervisor 并强制走代理..."
nohup env ALL_PROXY="${SOCKS_PROXY_ADDRESS}" python3 main_forecast.py > "${LOG_DIR}/main_forecast.log" 2>&1 &

# 启动 Main 服务 (main.py) with Supervisor


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
echo "- ${LOG_DIR}/main_supervisor.log (用于 main.py 重启事件)"

# 展示当前运行的 Python 进程（过滤掉 grep 自身）
echo -e "\n当前 Python 相关的后台进程："
ps -ef | grep python | grep -v grep