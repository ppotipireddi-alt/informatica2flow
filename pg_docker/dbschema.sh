#!/bin/bash

# Change to the directory where this script lives (supports running from any location)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

docker exec -i pgvector-db psql -U postgres < /Users/ppotipireddi/Cloudera/projects/AIAgents/mig_informatical_nifi/pg_docker/dbschema.sql    
