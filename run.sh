#!/bin/bash
cd "$(dirname "$0")"
mkdir -p logs
nohup uv run app_ver2/main.py > logs/app.log 2>&1 &
echo $! > logs/app.pid
echo "Started. PID: $(cat logs/app.pid)"
echo "Logs: tail -f logs/app.log"
