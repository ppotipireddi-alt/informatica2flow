#!/bin/bash

# Change to the directory where this script lives (supports running from any location)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Stop the container
docker compose -f docker-compose.yml down
