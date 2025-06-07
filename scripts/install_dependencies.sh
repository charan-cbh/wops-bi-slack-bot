#!/bin/bash
cd /home/ubuntu/wops-bi-slack-bot

# Create venv if not exists
if [ ! -d "/home/ubuntu/venv" ]; then
  python3 -m venv /home/ubuntu/venv
fi

source /home/ubuntu/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt