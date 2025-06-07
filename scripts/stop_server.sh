#!/bin/bash
# Stop any running instance of uvicorn
pkill -f "uvicorn main:app" || true