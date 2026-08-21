#!/bin/bash
. /etc/profile
# . /.bash_profile

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$script_dir" || exit 1

if [ -f "$script_dir/.env.local" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$script_dir/.env.local"
    set +a
fi

lark_url="${LARK_WEBHOOK_URL:-}"
python_bin="${PYTHON_BIN:-python3}"

send_lark_message() {
    local message=$1
    if [ -z "$lark_url" ]; then
        return 0
    fi
    curl -X POST -H "Content-Type: application/json" \
         -d "{\"msg_type\":\"interactive\",\"card\":{\"elements\":[{\"tag\":\"markdown\",\"content\":\"${message}\"}]}}" \
         "$lark_url"
}

is_script_running() {
    local script_name=$(basename "$1")
    pgrep -f "$script_name" >/dev/null 2>&1
}

start_script() {
    local script_path="$1"
    "$python_bin" "$script_path" &
}

monitor_scripts() {
    for script_path in "$@"; do
        if ! is_script_running "$script_path"; then
            send_lark_message "Script at $script_path is not running. Restarting..."
            start_script "$script_path"
        fi
    done
}

# 脚本路径列表
scripts_to_monitor=("$script_dir/bot_threshold_super_v4_1.py")

# 运行监控
monitor_scripts "${scripts_to_monitor[@]}"
