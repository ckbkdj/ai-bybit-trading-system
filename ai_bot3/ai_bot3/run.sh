#!/bin/bash

# 确保 logs 目录存在，如果不存在则创建它
mkdir -p logs

# 启动爆仓图爬虫
echo "启动爆仓图爬虫..."
nohup python3 liqmap_fetcher.py > logs/liqmap_fetcher.log 2>&1 &
sleep 1 # 短暂暂停，让进程有时间启动

# 启动 FastAPI 服务
echo "启动 API Server..."
# 请确保 api/api_server.py 和其依赖的 xgboost_trainer.py 的路径问题已解决
nohup python3 api/api_server.py > logs/api_server.log 2>&1 &
sleep 1 # 短暂暂停
echo "启动 Web Server..."
# 请确保 api/api_server.py 和其依赖的 xgboost_trainer.py 的路径问题已解决
nohup python3 api/web_server.py > logs/web_server.log 2>&1 &
sleep 1 # 短暂暂停

# 启动 Main 服务 (使用单独的日志文件并守护)
echo "启动 Main 服务..."
# 使用一个无限循环来守护 main.py 进程
# 如果 main.py 意外终止，它将在短暂延迟后自动重启
(
    # 将整个 while 循环作为一个子shell运行，并将其放入后台
    while true; do
        # 记录每次尝试启动的时间，写入守护日志
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 尝试启动 main.py..." | tee -a logs/main_supervisor.log

        # 使用 nohup 运行 main.py，将其标准输出和标准错误重定向到 main.log
        # 当 main.py 进程退出（无论是正常结束还是崩溃），控制权会回到这里
        nohup python3 main.py > logs/main.log 2>&1

        # 记录 main.py 进程停止的时间，写入守护日志
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] main.py 已停止，将在 5 秒后尝试重启..." | tee -a logs/main_supervisor.log
        sleep 5 # 暂停 5 秒后再次尝试启动
    done
) & # 将整个 (while true; do ... done) 结构放入后台运行

# 提示信息
echo "所有服务已尝试启动，请检查各自的日志文件以确认状态。"

# 展示当前运行的 Python 进程（过滤掉 grep 自身）
echo -e "\n当前 Python 相关的后台进程："
ps -ef | grep python | grep -v grep
##!/bin/bash
#
## 启动爬虫
#echo "启动爆仓图爬虫..."
#nohup python liqmap_fetcher.py > logs/liqmap_fetcher.log 2>&1 &
#echo "启动 Main..."
#nohup python main.py > logs/main.log 2>&1 &
## 启动 FastAPI 服务
#echo "启动 API Server..."
#nohup python api/api_server.py > logs/api_server.log 2>&1 &
#
## 提示
#echo "系统已启动，爆仓图爬虫与 API Server 正在运行。"
#
## 展示进程
#ps -ef | grep python