#!/bin/bash

# Change to the directory where this script lives (supports running from any location)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Start the container
docker compose -f docker-compose.yml up -d

# https://localhost:8443/nifi (HTTPS is used by default)
#or
# http://localhost:8080/nifi (if you configured HTTP access, as in the example above) 
# username: admin
# password: admin

