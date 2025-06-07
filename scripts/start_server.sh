#!/bin/bash
cd /home/ec2-user/wops-bi-slack-bot || exit 1
source /home/ec2-user/venv/bin/activate || exit 1

# Kill old uvicorn if running
pkill -f "uvicorn app.main:app" || true

# Start app
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > app.log 2>&1 &