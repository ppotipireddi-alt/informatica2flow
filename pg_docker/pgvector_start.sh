#!/bin/bash

# Change to the directory where this script lives (supports running from any location)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Start the container
docker compose -f docker-compose.yml up -d

# Connect to the database
## docker exec -it pgvector-db psql -U postgres
#docker exec -i pgvector-db psql -U postgres < /Users/ppotipireddi/Cloudera/projects/AIAgents/mig_informatical_nifi/pg_docker/dbschema.sql
