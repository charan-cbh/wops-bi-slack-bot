#!/bin/bash
cd /home/ec2-user/wops-bi-slack-bot
source /home/ec2-user/venv/bin/activate

# Kill previous uvicorn process if running
pkill -f "uvicorn app.main:app" || true

# Run app in background
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > app.log 2>&1 &