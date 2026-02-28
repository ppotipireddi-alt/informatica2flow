"""
expression_translator.py
------------------------
Translates Informatica PowerCenter expressions to NiFi-compatible equivalents.

Priority order:
  1. Deterministic rule lookup (from rule_memory_engine)
  2. Semantic vector search (from rule_memory_engine)
  3. Regex-based pattern rewriting (fast, no DB needed)
  4. LLM fallback (GPT-4o-mini)

Supports conversion to:
  - NiFi Expression Language (EL) — for RouteOnAttribute, UpdateAttribute
  - SQL (QueryRecord) — for QueryRecord processors
  - Groovy — for ExecuteGroovyScript (complex logic)
"""

import re
import os
import logging
from typing import Optional, Tuple
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("ExpressionTranslator")

# ─────────────────────────────────────────────────────────────────────────────
# Regex-based Deterministic Translation Rules
# Order matters: apply most-specific patterns first.
# ─────────────────────────────────────────────────────────────────────────────

# Each entry: (informatica_regex_pattern, sql_replacement_template, nifi_el_replacement)
_REGEX_RULES = [
    # IIF(condition, true, false) → CASE WHEN condition THEN true ELSE false END
    (
        r"IIF\s*\((.+?),\s*(.+?),\s*(.+?)\)",
        r"CASE WHEN \1 THEN \2 ELSE \3 END",
        r"${'\1':equals('\2'):ifElse('\2', '\3')}"   # simplified NiFi EL
    ),

    # DECODE(field, val1, out1, val2, out2, default)
    # → CASE WHEN field=val1 THEN out1 WHEN field=val2 THEN out2 ELSE default END
    (
        r"DECODE\s*\(([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*(.+?)\)",
        r"CASE WHEN \1=\2 THEN \3 WHEN \1=\4 THEN \5 ELSE \6 END",
        r"${'\1':equals('\2'):ifElse('\3', ${'\1':equals('\4'):ifElse('\5', '\6')})}"
    ),

    # SYSDATE → NOW()
    (r"\bSYSDATE\b", r"NOW()", r"${now():toNumber()}"),

    # TO_DATE(expr, format) → CAST(expr AS TIMESTAMP) or TO_TIMESTAMP
    (r"TO_DATE\s*\((.+?),\s*'(.+?)'\)", r"TO_TIMESTAMP(\1, '\2')", r"${'\1':toDate('\2')}"),

    # TRUNC(expr, scale) → ROUND(expr, scale)  [numeric truncation]
    (r"TRUNC\s*\((.+?),\s*(\d+)\)", r"ROUND(\1, \2)", r"${'\1':math('round')}"),
    # TRUNC(date, 'MM') → DATE_TRUNC('month', date)
    (r"TRUNC\s*\((.+?),\s*'MM'\)", r"DATE_TRUNC('month', \1)", r"${'\1':format('yyyy-MM-01')}"),
    (r"TRUNC\s*\((.+?),\s*'DD'\)", r"DATE_TRUNC('day', \1)", r"${'\1':format('yyyy-MM-dd')}"),
    (r"TRUNC\s*\((.+?),\s*'YY'\)", r"DATE_TRUNC('year', \1)", r"${'\1':format('yyyy-01-01')}"),
    (r"TRUNC\s*\((.+?)\)", r"DATE_TRUNC('day', \1)", r"${'\1':format('yyyy-MM-dd')}"),  # generic trunc → truncate to day

    # INSTR(str, substr) → POSITION(substr IN str)
    (r"INSTR\s*\((.+?),\s*(.+?)\)", r"POSITION(\2 IN \1)", r"${'\1':indexOf('\2')}"),

    # SUBSTR(str, start, len) → SUBSTRING(str, start, len)
    (r"SUBSTR\s*\((.+?),\s*(\d+),\s*(\d+)\)", r"SUBSTRING(\1, \2, \3)", r"${'\1':substring(\2, \3)}"),
    (r"SUBSTR\s*\((.+?),\s*(\d+)\)", r"SUBSTRING(\1, \2)", r"${'\1':substring(\2)}"),

    # LTRIM / RTRIM / TRIM
    (r"LTRIM\s*\((.+?)\)", r"LTRIM(\1)", r"${'\1':trimLeft()}"),
    (r"RTRIM\s*\((.+?)\)", r"RTRIM(\1)", r"${'\1':trimRight()}"),
    (r"TRIM\s*\((.+?)\)", r"TRIM(\1)", r"${'\1':trim()}"),

    # LENGTH / LEN
    (r"\bLENGTH\s*\((.+?)\)", r"CHAR_LENGTH(\1)", r"${'\1':length()}"),
    (r"\bLEN\s*\((.+?)\)", r"CHAR_LENGTH(\1)", r"${'\1':length()}"),

    # UPPER / LOWER
    (r"\bUPPER\s*\((.+?)\)", r"UPPER(\1)", r"${'\1':toUpper()}"),
    (r"\bLOWER\s*\((.+?)\)", r"LOWER(\1)", r"${'\1':toLower()}"),

    # CONCAT / string concatenation (||)
    (r"CONCAT\s*\((.+?),\s*(.+?)\)", r"CONCAT(\1, \2)", r"${'\1'}${'\2'}"),
    (r"(.+?)\s*\|\|\s*(.+)", r"CONCAT(\1, \2)", r"${'\1'}${'\2'}"),

    # ROUND(expr, scale)
    (r"ROUND\s*\((.+?),\s*(\d+)\)", r"ROUND(\1, \2)", r"${'\1':math('round')}"),

    # ABS
    (r"\bABS\s*\((.+?)\)", r"ABS(\1)", r"${'\1':math('abs')}"),

    # MOD
    (r"\bMOD\s*\((.+?),\s*(.+?)\)", r"MOD(\1, \2)", r"${'\1':math('mod')}"),

    # NVL(field, default) → COALESCE
    (r"\bNVL\s*\((.+?),\s*(.+?)\)", r"COALESCE(\1, \2)", r"${'\1':isNull():ifElse('\2', '\1')}"),

    # ISNULL
    (r"\bISNULL\s*\((.+?)\)", r"\1 IS NULL", r"${'\1':isNull()}"),

    # TO_CHAR(number, format) → CAST AS VARCHAR / TO_CHAR
    (r"TO_CHAR\s*\((.+?),\s*'(.+?)'\)", r"TO_CHAR(\1, '\2')", r"${'\1':format('\2')}"),
    (r"TO_CHAR\s*\((.+?)\)", r"CAST(\1 AS VARCHAR)", r"${'\1':toString()}"),

    # TO_INTEGER
    (r"TO_INTEGER\s*\((.+?)\)", r"CAST(\1 AS INTEGER)", r"${'\1':toNumber()}"),

    # TO_DECIMAL
    (r"TO_DECIMAL\s*\((.+?),\s*(\d+),\s*(\d+)\)", r"CAST(\1 AS DECIMAL(\2,\3))", r"${'\1':toNumber()}"),

    # ADD_TO_DATE(date, 'DD', n) → date + INTERVAL n DAYS
    (r"ADD_TO_DATE\s*\((.+?),\s*'DD',\s*(.+?)\)", r"\1 + INTERVAL '\2' DAY", r"${'\1':toDate():toNumber()}"),
    (r"ADD_TO_DATE\s*\((.+?),\s*'MM',\s*(.+?)\)", r"\1 + INTERVAL '\2' MONTH", r"${'\1':toDate():toNumber()}"),
    (r"ADD_TO_DATE\s*\((.+?),\s*'YY',\s*(.+?)\)", r"\1 + INTERVAL '\2' YEAR", r"${'\1':toDate():toNumber()}"),

    # DATE_DIFF
    (r"DATE_DIFF\s*\((.+?),\s*(.+?),\s*'DD'\)", r"DATEDIFF(DAY, \2, \1)", r"${'\1':toDate():toNumber()}"),

    # GET_DATE_PART
    (r"GET_DATE_PART\s*\((.+?),\s*'YEAR'\)", r"YEAR(\1)", r"${'\1':format('yyyy')}"),
    (r"GET_DATE_PART\s*\((.+?),\s*'MONTH'\)", r"MONTH(\1)", r"${'\1':format('MM')}"),
    (r"GET_DATE_PART\s*\((.+?),\s*'DAY'\)", r"DAY(\1)", r"${'\1':format('dd')}"),
]

# Compile all patterns
_COMPILED_RULES = [
    (re.compile(pat, re.IGNORECASE), sql_repl, el_repl)
    for pat, sql_repl, el_repl in _REGEX_RULES
]


# ─────────────────────────────────────────────────────────────────────────────
# LLM Few-Shot Prompt
# ─────────────────────────────────────────────────────────────────────────────

_LLM_SYSTEM_PROMPT = """You are an expert ETL migration engineer specializing in converting 
Informatica PowerCenter expressions to Apache NiFi-compatible SQL (for QueryRecord processors).

Rules:
- Output ONLY the translated SQL expression, no explanation
- Use standard ANSI SQL where possible
- For conditional logic: use CASE WHEN ... THEN ... ELSE ... END
- For null checks: use COALESCE or IS NULL
- For string ops: use standard SQL functions (UPPER, LOWER, TRIM, SUBSTRING, etc.)
- For date ops: use TO_TIMESTAMP, DATE_TRUNC, DATEDIFF, EXTRACT
- Keep field names unchanged (they map 1:1 from Informatica ports)
- If expression cannot be translated to SQL, output: UNSUPPORTED: <reason>

Examples:
Informatica: IIF(STATUS = 'A', 1, 0)
SQL: CASE WHEN STATUS = 'A' THEN 1 ELSE 0 END

Informatica: SYSDATE
SQL: NOW()

Informatica: TO_DATE(LOAD_DATE, 'YYYY-MM-DD')
SQL: TO_TIMESTAMP(LOAD_DATE, 'YYYY-MM-DD')

Informatica: TRUNC(AMT * QTY, 2)
SQL: ROUND(AMT * QTY, 2)

Informatica: NVL(DISCOUNT, 0)
SQL: COALESCE(DISCOUNT, 0)
"""


# ─────────────────────────────────────────────────────────────────────────────
# ExpressionTranslator
# ─────────────────────────────────────────────────────────────────────────────

class ExpressionTranslator:
    """
    Translates Informatica expressions to NiFi SQL / EL equivalents.

    Falls back from fastest to slowest:
      deterministic DB rules → local regex → LLM
    """

    def __init__(self, rule_engine=None, use_llm: bool = True):
        self.rule_engine = rule_engine
        self.use_llm = use_llm
        self._llm_client = None   # lazy init

    def _get_llm_client(self):
        if self._llm_client is None:
            try:
                from openai import OpenAI
                api_key = os.getenv("OPENAI_API_KEY")
                if not api_key or api_key == "CHANGE_ME":
                    logger.warning("OPENAI_API_KEY not set — LLM fallback disabled")
                    return None
                self._llm_client = OpenAI(api_key=api_key)
            except ImportError:
                logger.warning("openai package not installed — LLM fallback disabled")
        return self._llm_client

    # ── Step 1: Deterministic DB rules ────────────────────────────────────────
    def _try_deterministic(self, expression: str) -> Optional[Tuple[str, float]]:
        if not self.rule_engine:
            return None
        try:
            result = self.rule_engine.deterministic_lookup(expression)
            if result:
                return result, 0.97
        except Exception as e:
            logger.debug(f"Deterministic lookup failed: {e}")
        return None

    # ── Step 2: Semantic vector search ────────────────────────────────────────
    def _try_semantic(self, expression: str) -> Optional[Tuple[str, float]]:
        if not self.rule_engine:
            return None
        try:
            result = self.rule_engine.semantic_lookup(expression)
            if result:
                return result  # (nifi_logic, similarity)
        except Exception as e:
            logger.debug(f"Semantic lookup failed: {e}")
        return None

    # ── Step 3: Local regex rules ─────────────────────────────────────────────
    def _try_regex(self, expression: str, target: str = "sql") -> Optional[Tuple[str, float]]:
        """
        Args:
            target: "sql" or "el" (NiFi Expression Language)
        """
        repl_idx = 1 if target == "sql" else 2
        current = expression
        changed = False

        for compiled_pat, sql_repl, el_repl in _COMPILED_RULES:
            repl = sql_repl if target == "sql" else el_repl
            new_expr = compiled_pat.sub(repl, current)
            if new_expr != current:
                current = new_expr
                changed = True

        if changed:
            return current, 0.88
        return None

    # ── Step 4: LLM ───────────────────────────────────────────────────────────
    def _try_llm(self, expression: str, context: str = "") -> Optional[Tuple[str, float]]:
        if not self.use_llm:
            return None
        client = self._get_llm_client()
        if not client:
            return None

        model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        temperature = float(os.getenv("LLM_TEMPERATURE", "0.2"))
        timeout = float(os.getenv("LLM_TIMEOUT", "60"))

        user_msg = f"Informatica expression: {expression}"
        if context:
            user_msg += f"\nContext: {context}"

        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                temperature=temperature,
                timeout=timeout,
                max_tokens=256,
            )
            result = response.choices[0].message.content.strip()

            if result.startswith("UNSUPPORTED:"):
                logger.warning(f"LLM marked as unsupported: {result}")
                return expression, 0.3  # Return original with low confidence

            return result, 0.80
        except Exception as e:
            logger.error(f"LLM translation failed: {e}")
            return None

    # ── Main entry point ──────────────────────────────────────────────────────
    def translate(
        self,
        expression: str,
        context: str = "",
        target: str = "sql"
    ) -> Tuple[str, float, str]:
        """
        Translate an Informatica expression.

        Args:
            expression: The Informatica expression string
            context: Optional context (transformation name, field name, etc.)
            target: "sql" or "el"

        Returns:
            (translated_expression, confidence_score, method_used)
        """
        if not expression or expression.strip() == "":
            return expression, 1.0, "passthrough"

        # Step 1: Deterministic rules
        det = self._try_deterministic(expression)
        if det:
            return det[0], det[1], "deterministic"

        # Step 2: Semantic search
        sem = self._try_semantic(expression)
        if sem:
            return sem[0], sem[1], "semantic"

        # Step 3: Regex rules
        regex = self._try_regex(expression, target=target)
        if regex:
            return regex[0], regex[1], "regex"

        # Step 4: LLM
        llm = self._try_llm(expression, context)
        if llm:
            return llm[0], llm[1], "llm"

        # No translation found — return as-is
        logger.warning(f"No translation found for: {expression!r}")
        return expression, 0.4, "passthrough"

    def translate_all(
        self,
        transformations: list,
        target: str = "sql"
    ) -> Tuple[list, list]:
        """
        Translate expressions for all transformations in a parsed mapping.

        Returns:
            (enriched_transformations, translation_log)
        """
        translation_log = []

        for trans in transformations:
            for port in trans.get("ports", []):
                expr = port.get("expression")
                if not expr:
                    continue

                ctx = f"{trans['name']}.{port['name']}"
                translated, confidence, method = self.translate(expr, context=ctx, target=target)

                port["expression_original"] = expr
                port["expression_translated"] = translated
                port["translation_confidence"] = confidence
                port["translation_method"] = method

                translation_log.append({
                    "transformation": trans["name"],
                    "port": port["name"],
                    "original": expr,
                    "translated": translated,
                    "confidence": confidence,
                    "method": method,
                })

                if confidence < 0.7:
                    logger.warning(
                        f"Low-confidence translation ({confidence:.2f}) for "
                        f"{trans['name']}.{port['name']}: {expr!r} → {translated!r}"
                    )

        return transformations, translation_log


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph Node
# ─────────────────────────────────────────────────────────────────────────────

def expression_translation_node(state: dict) -> dict:
    """LangGraph node: translates all Informatica expressions in parsed mapping."""
    from rule_memory_engine import RuleMemoryEngine
    from config import DB_CONFIG

    try:
        rule_engine = RuleMemoryEngine(DB_CONFIG)
    except Exception as e:
        logger.warning(f"Rule engine unavailable, using regex+LLM only: {e}")
        rule_engine = None

    translator = ExpressionTranslator(rule_engine=rule_engine, use_llm=True)
    parsed = state.get("parsed_mapping", {})

    enriched_transformations, translation_log = translator.translate_all(
        parsed.get("transformations", []),
        target="sql"
    )

    parsed["transformations"] = enriched_transformations
    state["parsed_mapping"] = parsed
    state["expression_translations"] = translation_log

    # Update confidence scores
    if translation_log:
        avg_conf = sum(t["confidence"] for t in translation_log) / len(translation_log)
        existing = state.get("confidence_scores", [])
        existing.append(avg_conf)
        state["confidence_scores"] = existing

    logger.info(f"Expression translation complete: {len(translation_log)} expressions translated.")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Quick test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    translator = ExpressionTranslator(rule_engine=None, use_llm=False)

    test_expressions = [
        ("IIF(STATUS = 'A', 1, 0)", "sql"),
        ("SYSDATE", "sql"),
        ("TO_DATE(LOAD_DATE, 'YYYY-MM-DD')", "sql"),
        ("TRUNC(AMT * QTY)", "sql"),
        ("NVL(DISCOUNT, 0)", "sql"),
        ("LTRIM(RTRIM(NAME))", "sql"),
        ("UPPER(SUBSTR(CODE, 1, 3))", "sql"),
        ("DECODE(TYPE, 'A', 'Active', 'I', 'Inactive', 'Unknown')", "sql"),
        ("ADD_TO_DATE(LOAD_DATE, 'DD', 30)", "sql"),
        ("GET_DATE_PART(BIRTH_DATE, 'YEAR')", "sql"),
    ]

    for expr, tgt in test_expressions:
        translated, conf, method = translator.translate(expr, target=tgt)
        print(f"[{method:15s}] ({conf:.2f}) {expr!r}")
        print(f"               → {translated!r}")
        print()
