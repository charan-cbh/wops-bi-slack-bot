#!/bin/bash

cd /home/ec2-user/wops-bi-slack-bot || exit 1
source /home/ec2-user/venv/bin/activate || exit 1

# Log to file to trace what’s happening
echo "[INFO] Starting FastAPI app at $(date)" >> app.log

# Kill old uvicorn if running
pkill -f "uvicorn app.main:app" || echo "[INFO] No existing uvicorn process found" >> app.log

# Start uvicorn
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 >> app.log 2>&1 &

# Log PID to check later
echo "[INFO] Started uvicorn with PID $!" >> app.log