#!/bin/bash

echo "尝试停止所有运行中的预测服务及相关子进程..."

# 定义您的主服务脚本的关键文件名或路径
MAIN_PROCESS_KEYWORDS=(
    "liqmap_fetcher.py"
    "main_forecast.py"
    "main_train.py"
    "api/api_server.py"
    "api/web_server.py"
)

# 定义 multiprocessing 相关的子进程的精确命令行特征
# 它们通常使用特定的 Python 解释器路径和 'multiprocessing' 内部模块
# 确保这里使用您的实际 Python 解释器完整路径
MULTIPROCESSING_PATTERNS=(
    "/root/.pyenv/versions/3.11.8/bin/python3 -c from multiprocessing.spawn import spawn_main"
    "/root/.pyenv/versions/3.11.8/bin/python3 -c from multiprocessing.resource_tracker import main"
)

# 组合所有需要处理的进程模式
ALL_PROCESS_PATTERNS=(
    "${MAIN_PROCESS_KEYWORDS[@]}"
    "${MULTIPROCESSING_PATTERNS[@]}"
)

# --- 阶段 1: 尝试优雅停止 (SIGTERM) ---
echo "--- 阶段 1: 尝试优雅停止 (SIGTERM) 所有相关进程 ---"

for pattern in "${ALL_PROCESS_PATTERNS[@]}"; do
    echo "尝试优雅停止进程匹配: ${pattern}"
    # pkill -TERM -f 会向所有匹配完整命令行字符串的进程发送 SIGTERM 信号
    pkill -TERM -f "${pattern}"
done

echo "已发送 SIGTERM 信号。等待 10 秒，让进程有机会优雅退出..."
sleep 10

# --- 阶段 2: 检查并强制杀死 (SIGKILL) 剩余进程 ---
echo -e "\n--- 阶段 2: 检查并强制杀死 (SIGKILL) 剩余进程 ---"

found_remaining=false
for pattern in "${ALL_PROCESS_PATTERNS[@]}"; do
    echo "检查并强制杀死进程匹配: ${pattern}"
    # pgrep -f 查找匹配完整命令行字符串的进程ID
    # 注意：这里我们不需要 grep -v grep，因为 pgrep 已经足够精确
    pids=$(pgrep -f "${pattern}")

    if [ -z "$pids" ]; then
        echo "  ${pattern} 未找到剩余进程。"
    else
        echo "  找到 ${pattern} 的剩余进程ID: ${pids}"
        # 强制杀死进程
        kill -KILL $pids
        echo "  已发送 SIGKILL 信号。"
        found_remaining=true
    fi
done

if [ "$found_remaining" = false ]; then
    echo "所有指定的服务和子进程似乎都已成功停止。"
else
    echo "部分进程可能需要进一步检查或手动干预。"
fi

echo -e "\n--- 所有服务停止尝试已完成。---"

# 最后显示所有与 pyenv Python 解释器相关的进程，确认是否都已停止
echo -e "\n当前剩余的来自 /root/.pyenv/versions/3.11.8/bin/python3 的进程："
ps -ef | grep "/root/.pyenv/versions/3.11.8/bin/python3" | grep -v grep