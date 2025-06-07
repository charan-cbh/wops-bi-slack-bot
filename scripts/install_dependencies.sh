#!/bin/bash

# Remove any previous files in the deploy path to prevent overwrite errors
rm -rf /home/ec2-user/wops-bi-slack-bot/*

# Recreate scripts directory (since we wiped it)
mkdir -p /home/ec2-user/wops-bi-slack-bot/scripts

cd /home/ec2-user/wops-bi-slack-bot

# Create venv if not exists
if [ ! -d "/ec2-user/ubuntu/venv" ]; then
  python3 -m venv /home/ec2-user/venv
fi

source /home/ec2-user/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt