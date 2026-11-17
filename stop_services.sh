#!/usr/bin/env bash
# 停止三个服务及任何残留 worker 子进程
cd "$(dirname "$0")"
for f in data/control.pid data/ingest.pid data/query.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
  rm -f "$f"
done
pkill -f "app.worker" 2>/dev/null || true
echo "services stopped"
