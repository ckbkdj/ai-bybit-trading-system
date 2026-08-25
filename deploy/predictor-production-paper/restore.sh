#!/usr/bin/env bash
set -euo pipefail
source_file="$1"
target_file="$2"
test -f /run/ai-bybit/maintenance-approved
case "$target_file" in /var/lib/ai-bybit/*) ;; *) echo "target outside service data root" >&2; exit 2;; esac
test ! -e "$target_file"
install -m 0600 "$source_file" "$target_file"
sqlite3 "$target_file" "PRAGMA quick_check;" | grep -qx ok
