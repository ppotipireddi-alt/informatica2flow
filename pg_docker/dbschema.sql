-- ---------------------------------------------------------
-- Database Schema (PostgreSQL + pgvector)
-- ---------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE deterministic_rules (
    id SERIAL PRIMARY KEY,
    inf_pattern TEXT,
    nifi_replacement TEXT,
    confidence FLOAT,
    approved BOOLEAN DEFAULT TRUE,
    usage_count INT DEFAULT 0,
    success_count INT DEFAULT 0
);

CREATE TABLE semantic_rules (
    id SERIAL PRIMARY KEY,
    inf_logic TEXT,
    nifi_logic TEXT,
    embedding VECTOR(1536),
    confidence FLOAT
);

CREATE TABLE human_feedback (
    id SERIAL PRIMARY KEY,
    original_logic TEXT,
    llm_output TEXT,
    corrected_output TEXT,
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);