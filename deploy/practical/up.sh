#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
role=${1:-}
action=${2:-up}
case "$role" in
  predictor) compose="$root/deploy/practical/predictor.compose.yml"; env_file=${PREDICTOR_ENV_FILE:-"$root/deploy/practical/predictor.env"} ;;
  executor) compose="$root/deploy/practical/executor.compose.yml"; env_file=${EXECUTOR_ENV_FILE:-"$root/deploy/practical/executor.env"} ;;
  lab) compose="$root/deploy/practical/shadow-lab.compose.yml"; env_file=${LAB_ENV_FILE:-"$root/deploy/practical/lab.env"} ;;
  *) echo "Usage: $0 predictor|executor|lab [up|down|config|logs]" >&2; exit 2 ;;
esac
command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 3; }

args=(docker compose)
if [[ -f "$env_file" ]]; then args+=(--env-file "$env_file"); fi
args+=(-f "$compose")

case "$action" in
  config) "${args[@]}" config ;;
  up)
    if [[ "$role" == predictor ]]; then
      "${args[@]}" --profile full --profile ops up -d --build
    else
      "${args[@]}" up -d --build
    fi
    ;;
  down) "${args[@]}" down ;;
  logs) "${args[@]}" logs -f --tail=200 ;;
  *) echo "Unknown action: $action" >&2; exit 4 ;;
esac
