#!/bin/bash
# Stop any running instance of uvicorn
pkill -f "uvicorn main:app" || true

rm -rf /home/ec2-user/wops-bi-slack-bot/*