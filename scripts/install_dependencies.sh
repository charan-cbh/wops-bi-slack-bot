#!/bin/bash
cd /home/ec2-user/wops-bi-slack-bot

# Create venv if not exists
if [ ! -d "/ec2-user/ubuntu/venv" ]; then
  python3 -m venv /home/ec2-user/venv
fi

source /home/ec2-user/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt