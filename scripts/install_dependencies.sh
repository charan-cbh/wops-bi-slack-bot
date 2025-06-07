#!/bin/bash
cd /home/ec2-user/wops-bi-slack-bot || exit 1

# Create venv if not exists
if [ ! -d "/home/ec2-user/venv" ]; then
  python3 -m venv /home/ec2-user/venv
fi

source /home/ec2-user/venv/bin/activate || exit 1

# Upgrade pip & install dependencies
pip install --upgrade pip
pip install -r requirements.txt