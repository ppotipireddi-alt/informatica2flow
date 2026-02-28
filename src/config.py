from dotenv import load_dotenv
import os

load_dotenv()


# ----------------------------
# Database Configuration
# ----------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# ----------------------------
# NiFi REST API Configuration
# ----------------------------
NIFI_CONFIG = {
    "url": os.getenv("NIFI_URL"),
    "username": os.getenv("NIFI_USER"),   # Optional
    "password": os.getenv("NIFI_PASS"),   # Optional
    "root_pg_id": os.getenv("NIFI_ROOT_PG")
}

# ----------------------------
# MCP Server Configuration
# ----------------------------
MCP_CONFIG = {
    "host": os.getenv("MCP_HOST"),
    "port": int(os.getenv("MCP_PORT")),
    "tool_registry": os.getenv("MCP_TOOL_REGISTRY"),
    "enable_vector_memory": os.getenv("MCP_VECTOR_MEMORY"),
    "vector_model_name": os.getenv("VECTOR_MODEL"),
    "log_level": os.getenv("MCP_LOG_LEVEL")
}

# ----------------------------
# Agent Defaults
# ----------------------------
AGENT_CONFIG = {
    "max_retries": int(os.getenv("AGENT_MAX_RETRIES")),
    "retry_delay_seconds": int(os.getenv("AGENT_RETRY_DELAY")),
    "confidence_threshold": float(os.getenv("AGENT_CONF_THRESHOLD")),
    "human_review_queue": os.getenv("AGENT_HUMAN_QUEUE")
}

# ----------------------------
# Logging Configuration
# ----------------------------
LOG_CONFIG = {
    "log_file": os.getenv("LOG_FILE"),
    "log_level": os.getenv("LOG_LEVEL"),
    "max_bytes": int(os.getenv("LOG_MAX_BYTES")),
    "backup_count": int(os.getenv("LOG_BACKUP_COUNT"))
}

# ----------------------------
# Miscellaneous
# ----------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
TEMP_DIR = os.getenv("TEMP_DIR")