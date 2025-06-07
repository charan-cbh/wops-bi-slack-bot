#!/bin/bash

cd /home/ec2-user/wops-bi-slack-bot || exit 1
source /home/ec2-user/venv/bin/activate || exit 1

echo "[INFO] Installing Python dependencies..."
pip install -r requirements.txt

echo "[INFO] Fixing permissions after install..."
sudo chown -R ec2-user:ec2-user /home/ec2-user/wops-bi-slack-bot
sudo chmod -R u+rw /home/ec2-user/wops-bi-slack-bot