#!/bin/bash

# Activate the environment and stop the running uvicorn process
cd /home/ec2-user/wops-bi-slack-bot
source /home/ec2-user/venv/bin/activate

# Kill uvicorn process safely
pkill -f "uvicorn app.main:app" || true