

import psycopg2
import numpy as np
from typing import Optional, Tuple
from sentence_transformers import SentenceTransformer


class RuleMemoryEngine:

    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    # --------------------------------------------------
    # Deterministic Rule Lookup
    # --------------------------------------------------
    def deterministic_lookup(self, expression: str) -> Optional[str]:

        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, nifi_replacement, confidence
            FROM deterministic_rules
            WHERE approved = TRUE
        """)

        rows = cur.fetchall()

        for rule_id, replacement, confidence in rows:
            if expression.strip().startswith(rule_id_pattern(expression)):
                self.increment_usage(rule_id)
                return replacement

        return None

    # --------------------------------------------------
    # Semantic Search
    # --------------------------------------------------
    def semantic_lookup(self, expression: str) -> Optional[Tuple[str, float]]:

        embedding = self.model.encode(expression).tolist()

        cur = self.conn.cursor()
        cur.execute("""
            SELECT nifi_logic, confidence,
                   embedding <-> %s::vector AS distance
            FROM semantic_rules
            ORDER BY distance ASC
            LIMIT 1
        """, (embedding,))

        result = cur.fetchone()

        if result:
            nifi_logic, confidence, distance = result
            similarity = 1 - distance

            if similarity > 0.80:
                return nifi_logic, similarity

        return None

    # --------------------------------------------------
    # Main Apply Function
    # --------------------------------------------------
    def apply(self, expression: str) -> Tuple[str, float]:

        # 1. Deterministic
        deterministic = self.deterministic_lookup(expression)
        if deterministic:
            return deterministic, 0.95

        # 2. Semantic
        semantic = self.semantic_lookup(expression)
        if semantic:
            return semantic

        # 3. No Rule Found
        return expression, 0.5

    # --------------------------------------------------
    # Store New Semantic Rule
    # --------------------------------------------------
    def store_semantic_rule(self, inf_logic: str, nifi_logic: str):

        embedding = self.model.encode(inf_logic).tolist()

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO semantic_rules (inf_logic, nifi_logic, embedding, confidence)
            VALUES (%s, %s, %s, %s)
        """, (inf_logic, nifi_logic, embedding, 0.7))

        self.conn.commit()

    # --------------------------------------------------
    # Human Feedback Injection
    # --------------------------------------------------
    def store_human_feedback(self, original, llm_output, corrected, reason):

        cur = self.conn.cursor()

        cur.execute("""
            INSERT INTO human_feedback
            (original_logic, llm_output, corrected_output, reason)
            VALUES (%s, %s, %s, %s)
        """, (original, llm_output, corrected, reason))

        self.conn.commit()

        # Promote to deterministic rule
        cur.execute("""
            INSERT INTO deterministic_rules
            (inf_pattern, nifi_replacement, confidence, approved)
            VALUES (%s, %s, %s, TRUE)
        """, (original, corrected, 0.9))

        self.conn.commit()

    # --------------------------------------------------
    # Usage Tracking
    # --------------------------------------------------
    def increment_usage(self, rule_id):
        cur = self.conn.cursor()
        cur.execute("""
            UPDATE deterministic_rules
            SET usage_count = usage_count + 1
            WHERE id = %s
        """, (rule_id,))
        self.conn.commit()