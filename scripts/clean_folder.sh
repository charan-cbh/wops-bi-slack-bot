#!/bin/bash

TARGET="/home/ec2-user/wops-bi-slack-bot"
echo "[INFO] Cleaning $TARGET"

# Delete everything inside TARGET except .env, venv, and .git
sudo find "$TARGET" -mindepth 1 \
  ! -name "venv" \
  ! -name ".env" \
  ! -name ".git" \
  ! -path "$TARGET/venv/*" \
  ! -path "$TARGET/.git/*" \
  ! -name "." \
  -exec rm -rf {} + 2>/dev/null || true