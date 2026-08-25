#!/usr/bin/env bash
set -euo pipefail
source_file="$1"
target_file="/var/lib/ai-bybit/executor/execution.sqlite3"
test -f /run/ai-bybit/maintenance-approved
test ! -e "$target_file"
install -m 0600 "$source_file" "$target_file"
sqlite3 "$target_file" "PRAGMA quick_check;" | grep -qx ok
