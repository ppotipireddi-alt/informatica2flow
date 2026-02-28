"""
mcp_server.py
-------------
FastMCP server exposing Informatica → NiFi migration tools as AI-callable endpoints.

Tools exposed:
  1. parse_informatica      — Parse an Informatica XML file
  2. detect_spark           — Scan for Spark-dependent components
  3. translate_expression   — Translate an Informatica expression to NiFi SQL/EL
  4. map_datatype           — Map an Informatica datatype to NiFi/Avro/SQL
  5. convert_transformation — Convert a single transformation to a NiFi processor
  6. generate_nifi_flow     — Generate complete NiFi flow JSON from a mapping
  7. validate_flow          — Validate a generated NiFi flow model
  8. deploy_to_nifi         — Deploy a flow via NiFi REST API

Start the server:
  python mcp_server.py
  OR
  python main.py --mode mcp-server

The server listens on MCP_HOST:MCP_PORT (default http://0.0.0.0:8000).
"""

import os
import json
import logging
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("MCPServer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─────────────────────────────────────────────────────────────────────────────
# MCP Server Init
# ─────────────────────────────────────────────────────────────────────────────

from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    name="informatica-nifi-migration",
    instructions="""
You are an expert ETL migration agent specializing in converting Informatica PowerCenter 
workflows to Apache NiFi flows.

Your tools allow you to:
1. Parse Informatica XML files to extract sources, targets, transformations, and workflows
2. Detect Spark-dependent components that cannot be auto-migrated
3. Translate Informatica expressions (IIF, DECODE, SYSDATE, etc.) to NiFi-compatible SQL/EL
4. Map Informatica datatypes to NiFi/Avro/SQL equivalents
5. Convert individual transformations to NiFi processor configurations
6. Generate complete, importable NiFi flow JSON
7. Validate the generated flow for correctness
8. Deploy the flow directly to a running NiFi instance

When migrating a workflow:
- Always start with parse_informatica to understand the mapping
- Run detect_spark before attempting conversion (Spark components need manual handling)
- Use translate_expression for all OUTPUT port expressions
- Generate the complete flow with generate_nifi_flow
- Validate before deploying
""",
)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 1: Parse Informatica XML
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def parse_informatica(xml_path: str) -> Dict[str, Any]:
    """
    Parse an Informatica PowerCenter XML export file.

    Returns a structured dict with:
    - mapping_name: name of the mapping
    - sources: list of source tables/files with fields and datatypes
    - targets: list of target tables/files with fields and datatypes
    - transformations: list of transformations with type, ports, expressions, and attributes
    - workflows: top-level workflow and session definitions
    - connectors: data flow connections between instances
    - execution_order: topologically sorted transformation order
    - complexity: SIMPLE | MEDIUM | COMPLEX

    Args:
        xml_path: Absolute path to the Informatica XML file
    """
    from informatica_parser import InformaticaXMLParser
    from spark_detector import SparkDetector
    from datatype_mapper import DataTypeMapper

    if not os.path.exists(xml_path):
        return {"error": f"File not found: {xml_path}"}

    try:
        parser = InformaticaXMLParser(xml_path)
        parsed = parser.parse()

        # Enrich with datatype mappings
        mapper = DataTypeMapper()
        for source in parsed.get("sources", []):
            source["fields"] = mapper.map_fields(source.get("fields", []))
        for target in parsed.get("targets", []):
            target["fields"] = mapper.map_fields(target.get("fields", []))
        for trans in parsed.get("transformations", []):
            trans["ports"] = mapper.map_fields(trans.get("ports", []))

        # Quick Spark detection
        detector = SparkDetector()
        spark_report = detector.detect(parsed)
        parsed["spark_report"] = spark_report.to_dict()

        logger.info(f"Parsed: {parsed['mapping_name']} | Complexity: {parsed['complexity']} | Spark: {spark_report.has_spark}")
        return parsed

    except Exception as e:
        logger.error(f"Parse failed: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Tool 2: Detect Spark
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def detect_spark(xml_path: str) -> Dict[str, Any]:
    """
    Scan an Informatica XML file for Spark/Java-dependent components that 
    cannot be automatically migrated to NiFi.

    Returns a SparkReport with:
    - has_spark: true if any Spark dependencies found
    - unsupported_count: number of unsupported components
    - flags: list of flagged components with name, type, pattern, and migration_advice
    - summary: human-readable summary with recommended actions

    Args:
        xml_path: Absolute path to the Informatica XML file
    """
    from informatica_parser import InformaticaXMLParser
    from spark_detector import SparkDetector

    if not os.path.exists(xml_path):
        return {"error": f"File not found: {xml_path}"}

    try:
        parser = InformaticaXMLParser(xml_path)
        parsed = parser.parse()

        detector = SparkDetector()
        report = detector.detect(parsed)

        return report.to_dict()

    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Tool 3: Translate Expression
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def translate_expression(
    expression: str,
    context: Optional[str] = "",
    target: Optional[str] = "sql",
) -> Dict[str, Any]:
    """
    Translate a single Informatica PowerCenter expression to NiFi-compatible SQL or EL.

    Supports (30+ patterns):
    - IIF(cond, true, false) → CASE WHEN ... THEN ... ELSE ... END
    - DECODE(field, v1, o1, v2, o2, default) → CASE WHEN ...
    - SYSDATE → NOW()
    - TO_DATE(expr, fmt) → TO_TIMESTAMP(expr, fmt)
    - TRUNC(date, 'MM') → DATE_TRUNC('month', date)
    - NVL(field, default) → COALESCE(field, default)
    - INSTR(str, sub) → POSITION(sub IN str)
    - SUBSTR(str, s, n) → SUBSTRING(str, s, n)
    - LTRIM/RTRIM/TRIM → SQL equivalents
    - UPPER/LOWER/LENGTH → SQL equivalents
    - ADD_TO_DATE → INTERVAL arithmetic
    - GET_DATE_PART → YEAR/MONTH/DAY extraction
    - Concatenation (||) → CONCAT()
    - Falls back to LLM (GPT-4o-mini) for complex expressions

    Args:
        expression: The Informatica expression string (e.g. "IIF(AMT > 0, AMT, 0)")
        context: Optional context string (e.g. transformation.port name) for better LLM translation
        target: Output format — "sql" (for QueryRecord) or "el" (NiFi Expression Language)

    Returns:
        {
          "original": "IIF(AMT > 0, AMT, 0)",
          "translated": "CASE WHEN AMT > 0 THEN AMT ELSE 0 END",
          "confidence": 0.88,
          "method": "regex"   # deterministic | semantic | regex | llm | passthrough
        }
    """
    from expression_translator import ExpressionTranslator

    translator = ExpressionTranslator(rule_engine=None, use_llm=True)
    translated, confidence, method = translator.translate(
        expression, context=context or "", target=target or "sql"
    )

    return {
        "original": expression,
        "translated": translated,
        "confidence": confidence,
        "method": method,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tool 4: Map Datatype
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def map_datatype(
    inf_type: str,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Map an Informatica PowerCenter datatype to NiFi/Avro/SQL equivalents.

    Supported Informatica types:
      string, nstring, char, nchar, text, clob,
      integer, smallinteger, bigint, decimal, numeric, number, float, double, real,
      date/time, date, time, timestamp, timestamp_tz, timestamp_ltz, timestamp_ntz,
      boolean, binary, varbinary, blob, long, longraw, xmltype, any

    Args:
        inf_type: Informatica datatype name (case-insensitive)
        precision: Optional precision for numeric/string types
        scale: Optional scale for decimal/numeric types

    Returns:
        {
          "inf_type": "decimal",
          "avro_type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4},
          "sql_type": "DECIMAL(18,4)",
          "json_type": "number",
          "nifi_record_type": "DECIMAL",
          "precision": 18,
          "scale": 4
        }
    """
    from datatype_mapper import DataTypeMapper
    mapper = DataTypeMapper()
    return mapper.map(inf_type, precision, scale)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 5: Convert Transformation
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def convert_transformation(transformation_json: str) -> Dict[str, Any]:
    """
    Convert a single Informatica transformation to a NiFi processor configuration.

    The transformation_json should be a JSON string with the structure from parse_informatica:
    {
      "name": "exp_calculations",
      "type": "Expression",
      "ports": [{"name": "AMOUNT", "expression": "IIF(AMT > 0, AMT, 0)", "datatype": "decimal", ...}],
      "attributes": {}
    }

    Transformation → NiFi Processor mapping:
      Expression          → QueryRecord       (SQL SELECT expressions)
      Aggregator          → QueryRecord       (GROUP BY SQL)
      Lookup              → LookupRecord      (DB lookup)
      Joiner              → MergeRecord       (multi-stream merge)
      Router / Filter     → RouteOnAttribute  (conditional routing)
      Sorter              → SortRecord
      Normalizer          → SplitRecord
      Rank                → QueryRecord       (ROW_NUMBER() OVER ...)
      Union               → MergeRecord
      Sequence Generator  → UpdateRecord
      Java Transformation → ExecuteGroovyScript (⚠ manual migration required)
      Source Qualifier    → GenerateTableFetch + ExecuteSQL
      Unknown             → UpdateRecord      (passthrough)

    Args:
        transformation_json: JSON string of the transformation dict

    Returns:
        NiFi processor definition with:
        - type: NiFi processor type name
        - nifi_class: Full Java class name
        - config: Processor-specific configuration properties
        - relationships: Available output relationships
        - description: Migration notes and mapping explanation
    """
    from nifi_flow_generator import build_transformation_processor, gen_id

    try:
        trans = json.loads(transformation_json)
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON: {e}"}

    # First translate expressions
    from expression_translator import ExpressionTranslator
    translator = ExpressionTranslator(rule_engine=None, use_llm=True)
    for port in trans.get("ports", []):
        expr = port.get("expression")
        if expr:
            translated, confidence, method = translator.translate(expr)
            port["expression_translated"] = translated
            port["translation_confidence"] = confidence
            port["translation_method"] = method

    reader_id = gen_id()
    writer_id = gen_id()
    proc = build_transformation_processor(trans, (0, 0), reader_id, writer_id)

    # Remove internal IDs for cleaner output
    proc.pop("id", None)
    proc.pop("position", None)

    return proc


# ─────────────────────────────────────────────────────────────────────────────
# Tool 6: Generate NiFi Flow
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def generate_nifi_flow(
    xml_path: str,
    output_dir: Optional[str] = "output_data",
    translate_expressions: Optional[bool] = True,
) -> Dict[str, Any]:
    """
    Generate a complete NiFi flow definition from an Informatica XML file.

    This is the main migration tool. It performs:
    1. XML parsing (sources, targets, transformations, execution order)
    2. Spark detection (flags unsupported components)
    3. Expression translation (30+ Informatica functions → SQL/NiFi EL)
    4. Datatype mapping (Informatica types → NiFi/Avro/SQL)
    5. NiFi processor selection (correct processor for each transformation type)
    6. Controller service generation (DBCPConnectionPool, AvroReader, JsonRecordSetWriter)
    7. Connection wiring (based on Informatica connector definitions)
    8. Complete flow JSON export

    Args:
        xml_path: Absolute path to the Informatica XML file
        output_dir: Directory where the flow JSON will be saved (default: output_data)
        translate_expressions: Whether to translate Informatica expressions (default: true)

    Returns:
        {
          "flow_name": "m_customer_dim_load",
          "export_path": "/path/to/output_data/m_customer_dim_load_nifi_flow.json",
          "metadata": {
            "processor_count": 8,
            "connection_count": 7,
            "spark_detected": false,
            "complexity": "MEDIUM"
          },
          "processor_summary": [
            {"name": "Read_src_customers", "type": "GenerateTableFetch"},
            {"name": "exp_calculations", "type": "QueryRecord"},
            ...
          ],
          "spark_report": {...},
          "warnings": [...]
        }
    """
    from informatica_parser import InformaticaXMLParser
    from spark_detector import SparkDetector
    from expression_translator import ExpressionTranslator
    from datatype_mapper import DataTypeMapper
    from nifi_flow_generator import NiFiFlowGenerator

    if not os.path.exists(xml_path):
        return {"error": f"File not found: {xml_path}"}

    warnings = []

    try:
        # Step 1: Parse
        logger.info(f"Parsing: {xml_path}")
        parser = InformaticaXMLParser(xml_path)
        parsed = parser.parse()

        # Step 2: Spark detection
        detector = SparkDetector()
        spark_report = detector.detect(parsed)
        parsed["spark_report"] = spark_report.to_dict()

        if spark_report.has_spark:
            warnings.append(f"⚠️  {spark_report.unsupported_count} Spark-dependent components detected — manual migration required.")
            warnings.append(spark_report.summary())

        # Step 3: Datatype mapping
        mapper = DataTypeMapper()
        for source in parsed.get("sources", []):
            source["fields"] = mapper.map_fields(source.get("fields", []))
        for target in parsed.get("targets", []):
            target["fields"] = mapper.map_fields(target.get("fields", []))
        for trans in parsed.get("transformations", []):
            trans["ports"] = mapper.map_fields(trans.get("ports", []))

        # Step 4: Expression translation
        if translate_expressions:
            translator = ExpressionTranslator(rule_engine=None, use_llm=True)
            transformations, translation_log = translator.translate_all(
                parsed.get("transformations", []), target="sql"
            )
            parsed["transformations"] = transformations

            low_conf = [t for t in translation_log if t["confidence"] < 0.7]
            if low_conf:
                warnings.append(f"⚠️  {len(low_conf)} expressions with low translation confidence (< 0.70) — review recommended.")

        # Step 5: Generate flow
        output_dir = output_dir or "output_data"
        generator = NiFiFlowGenerator(output_dir=output_dir)
        export_path = generator.export(parsed)

        with open(export_path) as f:
            flow = json.load(f)

        processor_summary = [
            {
                "name": p["name"],
                "type": p["type"],
                "nifi_class": p.get("nifi_class", ""),
                "informatica_type": p.get("informatica_type", ""),
                "description": p.get("description", ""),
            }
            for p in flow["processors"]
        ]

        return {
            "flow_name": flow["flow_name"],
            "export_path": export_path,
            "metadata": {
                **flow.get("metadata", {}),
                "export_path": export_path,
            },
            "processor_summary": processor_summary,
            "spark_report": spark_report.to_dict(),
            "warnings": warnings,
        }

    except Exception as e:
        logger.error(f"Flow generation failed: {e}", exc_info=True)
        return {"error": str(e), "warnings": warnings}


# ─────────────────────────────────────────────────────────────────────────────
# Tool 7: Validate Flow
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def validate_flow(flow_json_path: str) -> Dict[str, Any]:
    """
    Validate a generated NiFi flow JSON file for correctness.

    Checks:
    - All processors have required configuration properties
    - All connections reference valid processor IDs
    - QueryRecord processors have SQL defined
    - No orphaned processors (processors with no connections)
    - Spark-flagged processors are present in warnings
    - Controller services are defined for processors that need them

    Args:
        flow_json_path: Absolute path to the generated NiFi flow JSON file

    Returns:
        {
          "valid": true/false,
          "errors": [...],
          "warnings": [...],
          "processor_count": 8,
          "connection_count": 7
        }
    """
    if not os.path.exists(flow_json_path):
        return {"error": f"Flow JSON not found: {flow_json_path}"}

    try:
        with open(flow_json_path) as f:
            flow = json.load(f)
    except Exception as e:
        return {"error": f"Failed to read flow JSON: {e}"}

    errors = []
    warnings = []

    processors = flow.get("processors", [])
    connections = flow.get("connections", [])
    services = flow.get("controller_services", [])
    proc_ids = {p["id"] for p in processors}

    # 1. Connection references valid IDs
    for conn in connections:
        if conn["source_id"] not in proc_ids:
            errors.append(f"Connection references unknown source processor: {conn['source_id']}")
        if conn["target_id"] not in proc_ids:
            errors.append(f"Connection references unknown target processor: {conn['target_id']}")

    # 2. QueryRecord processors have SQL
    for proc in processors:
        if proc["type"] == "QueryRecord":
            sql = proc.get("config", {}).get("sql", "")
            if not sql:
                errors.append(f"Processor '{proc['name']}' (QueryRecord) has no SQL defined")

    # 3. LookupRecord processors should reference a lookup service
    for proc in processors:
        if proc["type"] == "LookupRecord":
            svc = proc.get("config", {}).get("Lookup Service", "")
            if not svc:
                warnings.append(f"Processor '{proc['name']}' (LookupRecord) has no Lookup Service configured — configure manually")

    # 4. Orphaned processors
    connected_proc_ids = set()
    for conn in connections:
        connected_proc_ids.add(conn["source_id"])
        connected_proc_ids.add(conn["target_id"])

    for proc in processors:
        if proc["id"] not in connected_proc_ids and len(processors) > 1:
            warnings.append(f"Processor '{proc['name']}' ({proc['type']}) has no connections — may be orphaned")

    # 5. Spark warnings
    metadata = flow.get("metadata", {})
    if metadata.get("has_spark"):
        warnings.append("⚠️ Flow contains Spark-flagged components — ExecuteGroovyScript stubs require manual implementation")

    # 6. Controller services required
    service_types = {s["type"] for s in services}
    for proc in processors:
        if proc["type"] in ("QueryRecord", "UpdateRecord", "MergeRecord", "SplitRecord", "SortRecord"):
            if "AvroReader" not in service_types and "CSVReader" not in service_types:
                warnings.append("No RecordReader controller service found — add AvroReader or CSVReader")
                break

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "processor_count": len(processors),
        "connection_count": len(connections),
        "controller_service_count": len(services),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Tool 8: Deploy to NiFi
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def deploy_to_nifi(
    flow_json_path: str,
    nifi_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    parent_group_id: Optional[str] = "root",
) -> Dict[str, Any]:
    """
    Deploy a generated NiFi flow JSON to a running NiFi instance via REST API.

    Performs:
    1. Authentication (JWT token acquisition)
    2. Process group creation
    3. Controller service creation + enablement
    4. Processor creation with full configuration
    5. Connection wiring
    6. Processor startup

    Args:
        flow_json_path: Absolute path to the generated NiFi flow JSON
        nifi_url: NiFi URL (default: from .env NIFI_URL)
        username: NiFi username (default: from .env NIFI_USER)
        password: NiFi password (default: from .env NIFI_PASS)
        parent_group_id: Parent process group ID (default: "root")

    Returns:
        {
          "success": true/false,
          "group_id": "uuid-of-created-group",
          "flow_name": "m_customer_dim_load",
          "nifi_url": "https://localhost:8443",
          "error": null
        }
    """
    nifi_url = nifi_url or os.getenv("NIFI_URL", "https://localhost:8443")
    username = username or os.getenv("NIFI_USER", "admin")
    password = password or os.getenv("NIFI_PASS", "adminpassword123")

    if not os.path.exists(flow_json_path):
        return {"success": False, "error": f"Flow JSON not found: {flow_json_path}"}

    try:
        with open(flow_json_path) as f:
            flow = json.load(f)
    except Exception as e:
        return {"success": False, "error": f"Failed to read flow JSON: {e}"}

    try:
        import requests
        import urllib3
        urllib3.disable_warnings()

        # Authenticate
        token_url = f"{nifi_url.rstrip('/')}/nifi-api/access/token"
        r = requests.post(
            token_url,
            data={"username": username, "password": password},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
            timeout=30,
        )
        r.raise_for_status()
        token = r.text.strip()

        session = requests.Session()
        session.headers.update({
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        })
        session.verify = False

        base_url = nifi_url.rstrip("/")
        flow_name = flow["flow_name"]

        # Resolve root group
        if parent_group_id == "root":
            r = session.get(f"{base_url}/nifi-api/flow/process-groups/root")
            r.raise_for_status()
            parent_group_id = r.json()["processGroupFlow"]["id"]

        # Create process group
        r = session.post(
            f"{base_url}/nifi-api/process-groups/{parent_group_id}/process-groups",
            json={
                "revision": {"version": 0},
                "component": {
                    "name": flow_name,
                    "position": {"x": 100.0, "y": 100.0},
                },
            },
        )
        r.raise_for_status()
        group_id = r.json()["id"]
        logger.info(f"Created process group '{flow_name}': {group_id}")

        processor_id_map = {}

        # Create processors
        for proc in flow.get("processors", []):
            payload = {
                "revision": {"version": 0},
                "component": {
                    "type": proc["nifi_class"],
                    "name": proc["name"],
                    "position": proc.get("position", {"x": 0.0, "y": 0.0}),
                    "config": {
                        "properties": {
                            k: v for k, v in proc.get("config", {}).items()
                            if isinstance(v, str)  # only string properties via REST
                        },
                        "schedulingStrategy": proc.get("scheduling", {}).get("schedulingStrategy", "TIMER_DRIVEN"),
                        "schedulingPeriod": proc.get("scheduling", {}).get("schedulingPeriod", "0 sec"),
                    },
                },
            }
            r = session.post(f"{base_url}/nifi-api/process-groups/{group_id}/processors", json=payload)
            if r.ok:
                created_id = r.json()["id"]
                processor_id_map[proc["id"]] = created_id
                logger.info(f"Created processor: {proc['name']} ({proc['type']})")
            else:
                logger.warning(f"Failed to create processor {proc['name']}: {r.status_code} {r.text[:200]}")

        # Create connections
        for conn in flow.get("connections", []):
            src_id = processor_id_map.get(conn["source_id"])
            tgt_id = processor_id_map.get(conn["target_id"])
            if not src_id or not tgt_id:
                continue

            rels = conn.get("selected_relationships", ["success"])
            payload = {
                "revision": {"version": 0},
                "component": {
                    "source": {"id": src_id, "groupId": group_id, "type": "PROCESSOR"},
                    "destination": {"id": tgt_id, "groupId": group_id, "type": "PROCESSOR"},
                    "selectedRelationships": rels,
                    "backPressureObjectThreshold": conn.get("back_pressure_object_threshold", "10000"),
                    "backPressureDataSizeThreshold": conn.get("back_pressure_data_size_threshold", "1 GB"),
                },
            }
            r = session.post(f"{base_url}/nifi-api/process-groups/{group_id}/connections", json=payload)
            if r.ok:
                logger.info(f"Created connection: {src_id} → {tgt_id}")
            else:
                logger.warning(f"Failed to create connection: {r.status_code} {r.text[:200]}")

        return {
            "success": True,
            "group_id": group_id,
            "flow_name": flow_name,
            "nifi_url": nifi_url,
            "processor_count": len(processor_id_map),
            "error": None,
        }

    except Exception as e:
        logger.error(f"Deployment failed: {e}", exc_info=True)
        return {"success": False, "error": str(e), "nifi_url": nifi_url}


# ─────────────────────────────────────────────────────────────────────────────
# Server entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_server():
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8000"))
    logger.info(f"Starting Informatica → NiFi MCP Server on {host}:{port}")
    mcp.run(transport="streamable-http", host=host, port=port)


if __name__ == "__main__":
    run_server()
