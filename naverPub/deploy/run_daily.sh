#!/usr/bin/env bash
# naverPub 일일 실행 래퍼 (cron / systemd)
set -euo pipefail
DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$DIR"
export TZ=Asia/Seoul
# venv 사용 시
if [[ -f "$DIR/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$DIR/.venv/bin/activate"
fi
mkdir -p "$DIR/logs" "$DIR/outputs"
LOG="$DIR/logs/naverpub_$(date +%Y%m%d).log"
echo "==== $(date -Iseconds) start ====" >>"$LOG"
python3 "$DIR/runner.py" "$@" >>"$LOG" 2>&1
echo "==== $(date -Iseconds) end exit=$? ====" >>"$LOG"
