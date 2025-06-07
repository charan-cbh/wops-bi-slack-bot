#!/bin/bash

TARGET="/home/ec2-user/wops-bi-slack-bot"
echo "[INFO] Cleaning $TARGET"

# Delete everything except virtual env and .git, with sudo to avoid permission issues
sudo find "$TARGET" -mindepth 1 -not -name "venv" -not -name ".git" -exec rm -rf {} + 2>/dev/null || true