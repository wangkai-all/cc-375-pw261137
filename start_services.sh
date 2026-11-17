#!/usr/bin/env bash
# 启动三个独立进程：控制台(8101) / 接入服务(8102) / 查询端(8103)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data
python3 -m app.seed
python3 -m app.control_service >>data/control.log 2>&1 &
echo $! >data/control.pid
python3 -m app.ingest_service >>data/ingest.log 2>&1 &
echo $! >data/ingest.pid
python3 -m app.query_service >>data/query.log 2>&1 &
echo $! >data/query.pid
sleep 1.5
echo "控制台:    http://127.0.0.1:8101"
echo "接入服务:  http://127.0.0.1:8102/ingest/health"
echo "查询端:    http://127.0.0.1:8103/query/health"
echo "停止:      ./stop_services.sh"
