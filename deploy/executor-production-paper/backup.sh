#!/usr/bin/env bash
set -euo pipefail
destination="$1"
test -n "$destination"
install -d -m 0700 "$destination"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
sqlite3 /var/lib/ai-bybit/executor/execution.sqlite3 ".backup '$destination/execution-$timestamp.sqlite3'"
sha256sum "$destination/execution-$timestamp.sqlite3" > "$destination/SHA256SUMS-$timestamp"
