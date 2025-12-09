#!/bin/bash
cd "$(dirname "$0")"
if [ -f logs/app.pid ]; then
    kill $(cat logs/app.pid) 2>/dev/null && echo "Stopped" || echo "Not running"
    rm -f logs/app.pid
else
    echo "No PID file found"
fi
