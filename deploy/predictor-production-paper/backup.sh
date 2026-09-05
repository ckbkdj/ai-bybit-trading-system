#!/usr/bin/env bash
set -euo pipefail
destination="$1"
test -n "$destination"
install -d -m 0700 "$destination"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
sqlite3 /var/lib/ai-bybit/publication-worker/publication.sqlite3 ".backup '$destination/publication-$timestamp.sqlite3'"
sqlite3 /var/lib/ai-bybit/control-plane/control-plane.sqlite3 ".backup '$destination/control-plane-$timestamp.sqlite3'"
sqlite3 /var/lib/ai-bybit/market-collector/bybit-public.sqlite3 ".backup '$destination/market-collector-$timestamp.sqlite3'"
sha256sum "$destination"/*-"$timestamp".sqlite3 > "$destination/SHA256SUMS-$timestamp"
