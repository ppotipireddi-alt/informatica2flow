"""
datatype_mapper.py
------------------
Maps Informatica PowerCenter datatypes to:
  - NiFi/Avro schema types
  - SQL (JDBC) types
  - JSON schema types

Handles Oracle, SQL Server, MySQL, PostgreSQL type variants and
parameterized forms like VARCHAR2(100), NUMBER(10,2), etc.
"""

import re
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger("DataTypeMapper")


# ─────────────────────────────────────────────────────────────────────────────
# Mapping Tables
# ─────────────────────────────────────────────────────────────────────────────

_DTYPE_MAP: Dict[str, Dict[str, str]] = {
    # Standard string types
    "string":           {"avro": "string",  "sql": "VARCHAR",    "json": "string",  "nifi": "STRING"},
    "nstring":          {"avro": "string",  "sql": "NVARCHAR",   "json": "string",  "nifi": "STRING"},
    "char":             {"avro": "string",  "sql": "CHAR",       "json": "string",  "nifi": "STRING"},
    "nchar":            {"avro": "string",  "sql": "NCHAR",      "json": "string",  "nifi": "STRING"},
    "text":             {"avro": "string",  "sql": "TEXT",       "json": "string",  "nifi": "STRING"},
    "clob":             {"avro": "string",  "sql": "CLOB",       "json": "string",  "nifi": "STRING"},
    "nclob":            {"avro": "string",  "sql": "NCLOB",      "json": "string",  "nifi": "STRING"},
    "ntext":            {"avro": "string",  "sql": "NTEXT",      "json": "string",  "nifi": "STRING"},

    # Oracle aliases
    "varchar2":         {"avro": "string",  "sql": "VARCHAR",    "json": "string",  "nifi": "STRING"},
    "nvarchar2":        {"avro": "string",  "sql": "NVARCHAR",   "json": "string",  "nifi": "STRING"},
    "rowid":            {"avro": "string",  "sql": "VARCHAR(18)","json": "string",  "nifi": "STRING"},
    "urowid":           {"avro": "string",  "sql": "VARCHAR(4000)", "json": "string", "nifi": "STRING"},
    "xmltype":          {"avro": "string",  "sql": "CLOB",       "json": "string",  "nifi": "STRING"},
    "raw":              {"avro": "bytes",   "sql": "RAW",        "json": "string",  "nifi": "ARRAY"},
    "longraw":          {"avro": "bytes",   "sql": "LONGRAW",    "json": "string",  "nifi": "ARRAY"},

    # Numeric types
    "integer":          {"avro": "int",     "sql": "INTEGER",    "json": "integer", "nifi": "INT"},
    "smallinteger":     {"avro": "int",     "sql": "SMALLINT",   "json": "integer", "nifi": "SHORT"},
    "bigint":           {"avro": "long",    "sql": "BIGINT",     "json": "integer", "nifi": "LONG"},
    "decimal":          {"avro": "double",  "sql": "DECIMAL",    "json": "number",  "nifi": "DECIMAL"},
    "numeric":          {"avro": "double",  "sql": "NUMERIC",    "json": "number",  "nifi": "DECIMAL"},
    "number":           {"avro": "double",  "sql": "NUMERIC",    "json": "number",  "nifi": "DECIMAL"},
    "float":            {"avro": "float",   "sql": "FLOAT",      "json": "number",  "nifi": "FLOAT"},
    "double":           {"avro": "double",  "sql": "DOUBLE",     "json": "number",  "nifi": "DOUBLE"},
    "real":             {"avro": "float",   "sql": "REAL",       "json": "number",  "nifi": "FLOAT"},
    "int":              {"avro": "int",     "sql": "INTEGER",    "json": "integer", "nifi": "INT"},
    "smallint":         {"avro": "int",     "sql": "SMALLINT",   "json": "integer", "nifi": "SHORT"},
    "tinyint":          {"avro": "int",     "sql": "TINYINT",    "json": "integer", "nifi": "SHORT"},
    "long":             {"avro": "long",    "sql": "BIGINT",     "json": "integer", "nifi": "LONG"},
    "money":            {"avro": "double",  "sql": "DECIMAL(19,4)", "json": "number", "nifi": "DOUBLE"},
    "smallmoney":       {"avro": "float",   "sql": "DECIMAL(10,4)", "json": "number", "nifi": "FLOAT"},

    # Date/Time types
    "date/time":        {"avro": "long",    "sql": "TIMESTAMP",  "json": "string",  "nifi": "TIMESTAMP"},
    "date":             {"avro": "int",     "sql": "DATE",       "json": "string",  "nifi": "DATE"},
    "time":             {"avro": "int",     "sql": "TIME",       "json": "string",  "nifi": "TIME"},
    "timestamp":        {"avro": "long",    "sql": "TIMESTAMP",  "json": "string",  "nifi": "TIMESTAMP"},
    "datetime":         {"avro": "long",    "sql": "TIMESTAMP",  "json": "string",  "nifi": "TIMESTAMP"},
    "timestamp_tz":     {"avro": "long",    "sql": "TIMESTAMP WITH TIME ZONE", "json": "string", "nifi": "TIMESTAMP"},
    "timestamp_ltz":    {"avro": "long",    "sql": "TIMESTAMP WITH LOCAL TIME ZONE", "json": "string", "nifi": "TIMESTAMP"},
    "timestamp_ntz":    {"avro": "long",    "sql": "TIMESTAMP",  "json": "string",  "nifi": "TIMESTAMP"},
    "interval":         {"avro": "string",  "sql": "INTERVAL",   "json": "string",  "nifi": "STRING"},
    "milisecond":       {"avro": "long",    "sql": "BIGINT",     "json": "integer", "nifi": "LONG"},

    # Boolean
    "boolean":          {"avro": "boolean", "sql": "BOOLEAN",    "json": "boolean", "nifi": "BOOLEAN"},
    "bit":              {"avro": "boolean", "sql": "BIT",        "json": "boolean", "nifi": "BOOLEAN"},

    # Binary types
    "binary":           {"avro": "bytes",   "sql": "BINARY",     "json": "string",  "nifi": "ARRAY"},
    "varbinary":        {"avro": "bytes",   "sql": "VARBINARY",  "json": "string",  "nifi": "ARRAY"},
    "blob":             {"avro": "bytes",   "sql": "BLOB",       "json": "string",  "nifi": "ARRAY"},
    "bytea":            {"avro": "bytes",   "sql": "BYTEA",      "json": "string",  "nifi": "ARRAY"},

    # JSON / structured
    "json":             {"avro": "string",  "sql": "JSON",       "json": "object",  "nifi": "STRING"},
    "jsonb":            {"avro": "string",  "sql": "JSONB",      "json": "object",  "nifi": "STRING"},
    "any":              {"avro": "string",  "sql": "VARCHAR(4000)", "json": "string", "nifi": "STRING"},

    # PostgreSQL serials
    "serial":           {"avro": "int",     "sql": "SERIAL",     "json": "integer", "nifi": "INT"},
    "bigserial":        {"avro": "long",    "sql": "BIGSERIAL",  "json": "integer", "nifi": "LONG"},
    "uniqueidentifier": {"avro": "string",  "sql": "VARCHAR(36)","json": "string",  "nifi": "STRING"},
}


def _normalize_type(raw: str) -> str:
    """
    Normalize a raw database type string before lookup.
    Strips precision/scale parameters and lowercases.

    Examples:
      'VARCHAR2(100)' → 'varchar2'
      'NUMBER(10,2)'  → 'number'
      'number(p,s)'   → 'number'
      'CHAR(5)'       → 'char'
      'date/time'     → 'date/time'
    """
    key = raw.strip().lower()
    # Remove trailing parenthesized parameters containing digits, commas, or p/s placeholders
    key = re.sub(r'\s*\(\s*(?:[\d,\s]+|p(?:\s*,\s*s)?)\s*\)', '', key)
    return key.strip()


def _decimal_avro_with_precision(precision: int, scale: int) -> Dict[str, Any]:
    return {"type": "bytes", "logicalType": "decimal", "precision": precision, "scale": scale}


# ─────────────────────────────────────────────────────────────────────────────
# DataTypeMapper Class
# ─────────────────────────────────────────────────────────────────────────────

class DataTypeMapper:
    """Converts Informatica datatypes to NiFi-compatible equivalents."""

    def map(
        self,
        inf_type: str,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Map an Informatica type to NiFi/Avro/SQL equivalents.

        Handles parameterized forms: VARCHAR2(100), NUMBER(10,2), etc.
        """
        raw = inf_type or "string"
        key = _normalize_type(raw)
        entry = _DTYPE_MAP.get(key)

        if entry is None:
            logger.warning(f"Unknown Informatica type '{raw}' (normalized: '{key}'), defaulting to STRING")
            entry = {"avro": "string", "sql": "VARCHAR(4000)", "json": "string", "nifi": "STRING"}

        result: Dict[str, Any] = {
            "inf_type": raw,
            "avro_type": entry["avro"],
            "sql_type": entry["sql"],
            "json_type": entry["json"],
            "nifi_record_type": entry["nifi"],
            "precision": precision,
            "scale": scale,
        }

        # Apply precision/scale to SQL type
        if key in ("decimal", "numeric", "number") and precision is not None:
            if scale is not None:
                result["sql_type"] = f"DECIMAL({precision},{scale})"
                result["avro_type"] = _decimal_avro_with_precision(precision, scale)
            else:
                result["sql_type"] = f"DECIMAL({precision})"

        elif key in ("string", "nstring", "varchar2", "nvarchar2", "char", "nchar", "varchar") and precision is not None:
            result["sql_type"] = f"{entry['sql']}({precision})"

        elif key in ("varbinary", "binary") and precision is not None:
            result["sql_type"] = f"{entry['sql']}({precision})"

        return result

    def map_fields(self, fields: list) -> list:
        """Enrich a list of field dicts with type_mapping."""
        mapped = []
        for field in fields:
            f = dict(field)
            try:
                prec = int(f.get("precision")) if f.get("precision") else None
            except (ValueError, TypeError):
                prec = None
            try:
                scl = int(f.get("scale")) if f.get("scale") else None
            except (ValueError, TypeError):
                scl = None
            f["type_mapping"] = self.map(f.get("datatype", "string"), prec, scl)
            mapped.append(f)
        return mapped


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph Node
# ─────────────────────────────────────────────────────────────────────────────

def datatype_mapping_node(state: dict) -> dict:
    """LangGraph node: enriches parsed_mapping fields with type mappings."""
    mapper = DataTypeMapper()
    parsed = state.get("parsed_mapping", {})
    for source in parsed.get("sources", []):
        source["fields"] = mapper.map_fields(source.get("fields", []))
    for target in parsed.get("targets", []):
        target["fields"] = mapper.map_fields(target.get("fields", []))
    for trans in parsed.get("transformations", []):
        trans["ports"] = mapper.map_fields(trans.get("ports", []))
    state["parsed_mapping"] = parsed
    logger.info("Datatype mapping completed.")
    return state


if __name__ == "__main__":
    m = DataTypeMapper()
    test_types = [
        ("VARCHAR2(100)", None, None),
        ("NUMBER(10,2)", None, None),
        ("number(p,s)", None, None),
        ("date/time", None, None),
        ("datetime", None, None),
        ("decimal", 18, 4),
        ("binary", 255, None),
        ("string", 200, None),
        ("integer", None, None),
        ("bigint", None, None),
    ]
    for t, p, s in test_types:
        r = m.map(t, p, s)
        print(f"  {t:25} → SQL:{r['sql_type']:20} AVRO:{str(r['avro_type'])[:15]:15} NIFI:{r['nifi_record_type']}")
