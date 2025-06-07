#!/bin/bash
cd /home/ubuntu/wops-bi-slack-bot
source /home/ubuntu/venv/bin/activate

# Kill previous uvicorn process if running
pkill -f "uvicorn main:app" || true

# Run app in background
nohup uvicorn main:app --host 0.0.0.0 --port 8000 > app.log 2>&1 &