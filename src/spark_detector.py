"""
spark_detector.py
-----------------
Detects Spark-dependent components in an Informatica parsed mapping.

Informatica PowerCenter does NOT have native Spark support in the classic sense,
but:
  - Sessions with "Java Transformation" that contain Spark imports/API calls
  - Custom mapplets with embedded Spark/PySpark code
  - ELT-mode mappings targeting Spark engines
  - "Spark SQL" in TABLEATTRIBUTE or transformation descriptions

These are all unsupported NiFi targets and must be flagged for human review.
NiFi can handle many ETL patterns, but NOT:
  - Distributed Spark jobs (RDD/DataFrame API)
  - Spark Streaming
  - Complex Java transformations relying on Spark context
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any

logger = logging.getLogger("SparkDetector")


# ─────────────────────────────────────────────────────────────────────────────
# Spark Indicator Patterns
# ─────────────────────────────────────────────────────────────────────────────

_SPARK_INDICATORS = [
    # Type names
    r"\bSpark\b",
    r"\bPySpark\b",
    r"\bSparkSQL\b",
    r"\bSpark\s*Session\b",
    r"\bSparkContext\b",
    r"\bDataFrame\b",
    r"\bRDD\b",
    r"\bSparkConf\b",
    # Imports
    r"import\s+org\.apache\.spark",
    r"from\s+pyspark",
    r"import\s+pyspark",
    r"spark\.sql\(",
    r"spark\.read\(",
    r"spark\.write",
    # Informatica-specific ELT Spark type markers
    r"Java\s+Transformation",
    r"JAVASESSION",
    r"Spark\s+Engine",
    r"ELT\s+Mapping",
    r"\beltMode\b",
    r"org\.apache\.spark",
]

_COMPILED_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _SPARK_INDICATORS]


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SparkFlag:
    component_name: str
    component_type: str          # e.g. "Transformation", "Session", "Workflow"
    matched_pattern: str
    matched_text: str
    migration_advice: str


@dataclass
class SparkReport:
    has_spark: bool
    flags: List[SparkFlag] = field(default_factory=list)

    @property
    def unsupported_count(self) -> int:
        return len(self.flags)

    def summary(self) -> str:
        if not self.has_spark:
            return "✅ No Spark dependencies detected. Safe to migrate to NiFi."
        lines = [
            f"⚠️  {self.unsupported_count} Spark-dependent component(s) detected:",
        ]
        for f in self.flags:
            lines.append(
                f"   • [{f.component_type}] {f.component_name}: {f.matched_pattern!r}"
            )
        lines.append("")
        lines.append("These components CANNOT be auto-migrated to NiFi.")
        lines.append("Suggested actions:")
        lines.append("  1. Replace Spark logic with NiFi ExecuteGroovyScript or ExecuteScript processors.")
        lines.append("  2. Move heavy Spark workloads to an external Spark cluster and trigger via InvokeHTTP.")
        lines.append("  3. Rewrite using NiFi's QueryRecord + SQL-pushdown capabilities.")
        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "has_spark": self.has_spark,
            "unsupported_count": self.unsupported_count,
            "flags": [
                {
                    "component_name": f.component_name,
                    "component_type": f.component_type,
                    "matched_pattern": f.matched_pattern,
                    "matched_text": f.matched_text,
                    "migration_advice": f.migration_advice,
                }
                for f in self.flags
            ],
            "summary": self.summary(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# SparkDetector
# ─────────────────────────────────────────────────────────────────────────────

class SparkDetector:

    def _scan_text(self, text: str) -> List[str]:
        """Return list of pattern strings that matched."""
        matched = []
        for pat, raw in zip(_COMPILED_PATTERNS, _SPARK_INDICATORS):
            if pat.search(text):
                matched.append(raw)
        return matched

    def _check_component(
        self,
        name: str,
        comp_type: str,
        texts_to_scan: List[str],
        flags: List[SparkFlag],
    ):
        for text in texts_to_scan:
            if not text:
                continue
            matched_pats = self._scan_text(text)
            for pat in matched_pats:
                advice = self._generate_advice(comp_type, pat)
                flags.append(SparkFlag(
                    component_name=name,
                    component_type=comp_type,
                    matched_pattern=pat,
                    matched_text=text[:200],
                    migration_advice=advice,
                ))

    def _generate_advice(self, comp_type: str, pattern: str) -> str:
        if "Java" in pattern:
            return (
                "Java Transformation detected. Replace with ExecuteGroovyScript or "
                "ExecuteScript (Python/Groovy) in NiFi, or extract Java logic to a "
                "REST microservice and call it via InvokeHTTP."
            )
        if "spark" in pattern.lower() or "pyspark" in pattern.lower():
            return (
                "Spark/PySpark logic detected. This must be migrated to either: "
                "(a) NiFi ExecuteScript with Groovy/Python for small transforms, "
                "(b) an external Spark job triggered via InvokeHTTP, or "
                "(c) SQL pushdown via QueryRecord + JDBC."
            )
        if "RDD" in pattern or "DataFrame" in pattern:
            return (
                "Spark RDD/DataFrame API detected. NiFi does not support distributed "
                "data processing. Consider refactoring to SQL or use an external Spark cluster."
            )
        return (
            "Unsupported Spark-related pattern detected. Manual review and rewrite required."
        )

    def detect(self, parsed_mapping: Dict[str, Any]) -> SparkReport:
        """
        Scan a parsed Informatica mapping for Spark indicators.

        Args:
            parsed_mapping: Output from InformaticaXMLParser.parse()

        Returns:
            SparkReport
        """
        flags: List[SparkFlag] = []

        # ── Scan transformations ──────────────────────────────────────────────
        for trans in parsed_mapping.get("transformations", []):
            name = trans.get("name", "unknown")
            t_type = trans.get("type", "")
            description = trans.get("description", "") or ""
            texts = [t_type, description]

            # Also scan port expressions
            for port in trans.get("ports", []):
                expr = port.get("expression", "") or ""
                texts.append(expr)

            # Also scan table attributes
            for attr_val in trans.get("attributes", {}).values():
                texts.append(str(attr_val) if attr_val else "")

            self._check_component(name, "Transformation", texts, flags)

        # ── Scan workflows ────────────────────────────────────────────────────
        for wf in parsed_mapping.get("workflows", []):
            name = wf.get("name", "unknown")
            texts = [wf.get("type", ""), wf.get("description", "") or ""]
            for sess in wf.get("sessions", []):
                texts.append(sess.get("type", ""))
                texts.append(sess.get("session_type", ""))
                texts.append(sess.get("description", "") or "")
            self._check_component(name, "Workflow", texts, flags)

        # ── Scan source/target metadata ───────────────────────────────────────
        for src in parsed_mapping.get("sources", []):
            db_type = src.get("database_type", "") or ""
            if "spark" in db_type.lower():
                flags.append(SparkFlag(
                    component_name=src.get("name", "unknown"),
                    component_type="Source",
                    matched_pattern="Spark database type",
                    matched_text=db_type,
                    migration_advice="Replace Spark data source with appropriate NiFi source processor (GetFile, GenerateTableFetch, etc.)",
                ))

        for tgt in parsed_mapping.get("targets", []):
            db_type = tgt.get("database_type", "") or ""
            if "spark" in db_type.lower():
                flags.append(SparkFlag(
                    component_name=tgt.get("name", "unknown"),
                    component_type="Target",
                    matched_pattern="Spark database type",
                    matched_text=db_type,
                    migration_advice="Replace Spark target with NiFi PutDatabaseRecord or PutFile processor.",
                ))

        report = SparkReport(has_spark=len(flags) > 0, flags=flags)
        logger.info(f"Spark detection complete: has_spark={report.has_spark}, flags={report.unsupported_count}")
        return report


# ─────────────────────────────────────────────────────────────────────────────
# LangGraph Node
# ─────────────────────────────────────────────────────────────────────────────

def spark_detection_node(state: dict) -> dict:
    """LangGraph node: detects Spark dependencies in parsed mapping."""
    detector = SparkDetector()
    parsed = state.get("parsed_mapping", {})

    report = detector.detect(parsed)

    state["spark_report"] = report.to_dict()
    state["unsupported_items"] = report.flags

    if report.has_spark:
        state["review_required"] = True
        logger.warning(f"⚠️  Spark dependencies detected — routing to human review.\n{report.summary()}")
    else:
        logger.info(report.summary())

    return state


# ─────────────────────────────────────────────────────────────────────────────
# Quick test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sample = {
        "transformations": [
            {
                "name": "exp_calc",
                "type": "Expression",
                "description": None,
                "ports": [
                    {"name": "out_amt", "expression": "IIF(AMT > 0, AMT, 0)", "datatype": "decimal"}
                ],
                "attributes": {}
            },
            {
                "name": "java_transform",
                "type": "Java Transformation",
                "description": "Uses spark.sql() to process data",
                "ports": [],
                "attributes": {"Java Code": "import org.apache.spark.sql.SparkSession; ..."}
            }
        ],
        "workflows": [],
        "sources": [],
        "targets": [],
    }

    detector = SparkDetector()
    r = detector.detect(sample)
    print(r.summary())
    print("\nDict:", r.to_dict())
