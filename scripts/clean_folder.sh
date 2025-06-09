#!/bin/bash

TARGET="/home/ec2-user/wops-bi-slack-bot"
echo "[INFO] Cleaning $TARGET"

# Delete everything inside TARGET except .env, venv, .git, and rsa_key.p8
sudo find "$TARGET" -mindepth 1 \
  ! -name "venv" \
  ! -name ".env" \
  ! -name ".git" \
  ! -name "rsa_key.p8" \
  ! -path "$TARGET/venv/*" \
  ! -path "$TARGET/.git/*" \
  ! -name "." \
  -exec rm -rf {} + 2>/dev/null || true