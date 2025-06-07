#!/bin/bash

TARGET_DIR="/home/ec2-user/wops-bi-slack-bot"

echo "[INFO] Cleaning up old app files from $TARGET_DIR"

# Delete everything except the venv folder
find "$TARGET_DIR" -mindepth 1 -not -path "$TARGET_DIR/venv*" -exec rm -rf {} + 2>> "$TARGET_DIR/clean_errors.log"

echo "[INFO] Cleanup complete"