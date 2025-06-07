#!/bin/bash

echo "[INFO] Stopping any running Uvicorn processes..."
pkill -f "uvicorn app.main:app" || true