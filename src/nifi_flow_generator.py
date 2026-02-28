"""
nifi_flow_generator.py
----------------------
Generates a complete, importable Apache NiFi Flow Definition JSON
from the intermediate model produced by NiFiIntermediateConverter.

The output is a NiFi Process Group JSON that can be:
  1. Imported via NiFi UI → Upload Template (as XML via template_wrapper)
  2. Deployed directly via NiFi REST API POST /nifi-api/process-groups/{id}/process-groups

Architecture:
  - Each Informatica transformation becomes exactly one NiFi processor
  - Source qualifiers become GetFile/ExecuteSQL (based on source type)
  - Targets become PutDatabaseRecord/PutFile (based on target type)
  - Controller services (DBCPConnectionPool, AvroReader, JsonRecordSetWriter) are generated
  - Processors are laid out in a left-to-right grid

NiFi Processor Class Mapping (Informatica → NiFi):
  Expression        → QueryRecord         (SQL on FlowFile)
  Aggregator        → QueryRecord         (GROUP BY SQL on FlowFile)
  Lookup            → LookupRecord        (DB lookup via controller service)
  Joiner            → MergeRecord         (multi-stream merge) + QueryRecord (JOIN SQL)
  Router/Filter     → RouteOnAttribute    (NiFi EL conditions)
  Sorter            → SortRecord
  Normalizer        → SplitRecord
  Rank              → QueryRecord         (ROW_NUMBER() OVER ...)
  Union             → MergeRecord
  Sequence Gen.     → UpdateRecord
  Java/Spark        → ExecuteGroovyScript (⚠ flagged, manual migration needed)
  Source (DB)       → GenerateTableFetch + ExecuteSQL
  Source (File)     → ListFile + FetchFile
  Target (DB)       → PutDatabaseRecord
  Target (File)     → PutFile
  Default/Unknown   → UpdateRecord        (passthrough)
"""

import uuid
import json
import os
import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger("NiFiFlowGenerator")


# ─────────────────────────────────────────────────────────────────────────────
# NiFi Processor Full Class Name Registry
# ─────────────────────────────────────────────────────────────────────────────

PROCESSOR_CLASS_MAP = {
    # Core data transformation
    "QueryRecord":          "org.apache.nifi.processors.standard.QueryRecord",
    "LookupRecord":         "org.apache.nifi.processors.standard.LookupRecord",
    "RouteOnAttribute":     "org.apache.nifi.processors.standard.RouteOnAttribute",
    "UpdateRecord":         "org.apache.nifi.processors.standard.UpdateRecord",
    "UpdateAttribute":      "org.apache.nifi.processors.standard.UpdateAttribute",
    "MergeRecord":          "org.apache.nifi.processors.standard.MergeRecord",
    "SplitRecord":          "org.apache.nifi.processors.standard.SplitRecord",
    "SortRecord":           "org.apache.nifi.processors.standard.SortRecord",
    "ConvertRecord":        "org.apache.nifi.processors.standard.ConvertRecord",
    # File IO
    "GetFile":              "org.apache.nifi.processors.standard.GetFile",
    "ListFile":             "org.apache.nifi.processors.standard.ListFile",
    "FetchFile":            "org.apache.nifi.processors.standard.FetchFile",
    "PutFile":              "org.apache.nifi.processors.standard.PutFile",
    # Database IO
    "GenerateTableFetch":   "org.apache.nifi.processors.standard.GenerateTableFetch",
    "ExecuteSQL":           "org.apache.nifi.processors.standard.ExecuteSQL",
    "PutDatabaseRecord":    "org.apache.nifi.processors.standard.PutDatabaseRecord",
    "QueryDatabaseTable":   "org.apache.nifi.processors.standard.QueryDatabaseTable",
    # Scripting
    "ExecuteGroovyScript":  "org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
    "ExecuteScript":        "org.apache.nifi.processors.script.ExecuteScript",
    # Utility
    "LogMessage":           "org.apache.nifi.processors.standard.LogMessage",
    "Wait":                 "org.apache.nifi.processors.standard.Wait",
    "GenerateFlowFile":     "org.apache.nifi.processors.standard.GenerateFlowFile",
}

# Controller services
CONTROLLER_SERVICE_CLASS_MAP = {
    "DBCPConnectionPool":       "org.apache.nifi.dbcp.DBCPConnectionPool",
    "AvroReader":               "org.apache.nifi.avro.AvroReader",
    "JsonRecordSetWriter":      "org.apache.nifi.json.JsonRecordSetWriter",
    "CSVReader":                "org.apache.nifi.csv.CSVReader",
    "CSVRecordSetWriter":       "org.apache.nifi.csv.CSVRecordSetWriter",
    "DBLookupService":          "org.apache.nifi.lookup.db.DatabaseLookupService",
    "SimpleKeyValueLookupService": "org.apache.nifi.lookup.SimpleKeyValueLookupService",
}

# Relationships per processor type
PROCESSOR_RELATIONSHIPS = {
    "QueryRecord":          ["sql", "failure", "original"],  # NiFi uses dynamic rel named after SQL property key
    "LookupRecord":         ["success", "failure", "original", "unmatched"],
    "RouteOnAttribute":     ["unmatched"],  # + dynamic routes
    "UpdateRecord":         ["success", "failure"],
    "UpdateAttribute":      ["success"],
    "MergeRecord":          ["merged", "original"],
    "SplitRecord":          ["splits", "failure", "original"],
    "SortRecord":           ["success", "failure"],
    "ConvertRecord":        ["success", "failure"],
    "GetFile":              ["success"],
    "ListFile":             ["success"],
    "FetchFile":            ["success", "not.found", "permission.denied", "failure"],
    "PutFile":              ["success", "failure"],
    "GenerateTableFetch":   ["success"],
    "ExecuteSQL":           ["success", "failure"],
    "PutDatabaseRecord":    ["success", "failure", "retry"],
    "QueryDatabaseTable":   ["success"],
    "ExecuteGroovyScript":  ["success", "failure"],
    "ExecuteScript":        ["success", "failure"],
    "LogMessage":           ["success"],
    "GenerateFlowFile":     ["success"],
}


def gen_id() -> str:
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# Informatica Type → NiFi Processor Mapping
# ─────────────────────────────────────────────────────────────────────────────

def resolve_processor_type(transformation: Dict[str, Any]) -> Tuple[str, str]:
    """
    Given an Informatica transformation dict, return (processor_type, description).
    """
    t_type = (transformation.get("type") or "").strip()
    t_name = (transformation.get("name") or "").strip()

    MAPPING = {
        "Expression":             ("QueryRecord",         "Expression → SQL SELECT on FlowFile"),
        "Aggregator":             ("QueryRecord",         "Aggregator → GROUP BY SQL on FlowFile"),
        "Lookup Procedure":       ("LookupRecord",        "Lookup → LookupRecord with DB controller service"),
        "Lookup":                 ("LookupRecord",        "Lookup → LookupRecord with DB controller service"),
        "Joiner":                 ("MergeRecord",         "Joiner → MergeRecord (multi-stream merge)"),
        "Router":                 ("RouteOnAttribute",    "Router → RouteOnAttribute with NiFi EL conditions"),
        "Filter":                 ("RouteOnAttribute",    "Filter → RouteOnAttribute (keep/drop rows)"),
        "Sorter":                 ("SortRecord",          "Sorter → SortRecord"),
        "Normalizer":             ("SplitRecord",         "Normalizer → SplitRecord (row expansion)"),
        "Rank":                   ("QueryRecord",         "Rank → QueryRecord with ROW_NUMBER() OVER(...)"),
        "Union":                  ("MergeRecord",         "Union → MergeRecord (all mode)"),
        "Sequence Generator":     ("UpdateRecord",        "Sequence Generator → UpdateRecord with sequence value"),
        "Source Qualifier":       ("GenerateTableFetch",  "Source Qualifier → GenerateTableFetch + ExecuteSQL"),
        "Java Transformation":    ("ExecuteGroovyScript", "⚠ Java Transformation → ExecuteGroovyScript (UNSUPPORTED - manual migration required)"),
        "Custom Transformation":  ("ExecuteGroovyScript", "⚠ Custom Transformation → ExecuteGroovyScript"),
        "Mapplet":                ("UpdateRecord",        "Mapplet → UpdateRecord (simplified passthrough)"),
        "Stored Procedure":       ("ExecuteSQL",         "Stored Procedure → ExecuteSQL"),
    }

    for key, (proc_type, desc) in MAPPING.items():
        if key.lower() in t_type.lower():
            return proc_type, desc

    return "UpdateRecord", f"Unknown type '{t_type}' → UpdateRecord (passthrough)"


# ─────────────────────────────────────────────────────────────────────────────
# Source/Target Processor Builder
# ─────────────────────────────────────────────────────────────────────────────

def build_source_processor(source: Dict[str, Any], position: Tuple[int, int]) -> Dict[str, Any]:
    """Build a NiFi source processor from an Informatica source definition."""
    db_type = (source.get("database_type") or "").lower()
    name = source.get("name", "Source")

    if db_type in ("flat file", "file", "csv", "txt", "delimited"):
        proc_type = "GetFile"
        config = {
            "Input Directory": "/data/input",
            "File Filter": ".*",
            "Keep Source File": "false",
        }
    elif db_type in ("oracle", "microsoft sql server", "mysql", "postgresql", "db2", "sybase", "teradata", "odbc"):
        proc_type = "GenerateTableFetch"
        config = {
            "Table Name": name,
            "Maximum-value Columns": "LAST_UPDATED",
        }
    else:
        proc_type = "GenerateTableFetch"
        config = {"Table Name": name}

    return {
        "id": gen_id(),
        "type": proc_type,
        "name": f"Read_{name}",
        "nifi_class": PROCESSOR_CLASS_MAP[proc_type],
        "config": config,
        "position": {"x": position[0], "y": position[1]},
        "relationships": PROCESSOR_RELATIONSHIPS.get(proc_type, ["success"]),
        "source_ref": name,
        "scheduling": {"schedulingStrategy": "TIMER_DRIVEN", "schedulingPeriod": "0 sec"},
    }


def build_target_processor(target: Dict[str, Any], position: Tuple[int, int], reader_service_id: str) -> Dict[str, Any]:
    """Build a NiFi target processor from an Informatica target definition."""
    db_type = (target.get("database_type") or "").lower()
    name = target.get("name", "Target")

    if db_type in ("flat file", "file", "csv", "txt", "delimited"):
        proc_type = "PutFile"
        config = {
            "Directory": "/data/output",
            "Conflict Resolution Strategy": "replace",
        }
    else:
        proc_type = "PutDatabaseRecord"
        config = {
            "Table Name": name,
            "Statement Type": "INSERT",
            "Record Reader": reader_service_id,
        }

    return {
        "id": gen_id(),
        "type": proc_type,
        "name": f"Write_{name}",
        "nifi_class": PROCESSOR_CLASS_MAP[proc_type],
        "config": config,
        "position": {"x": position[0], "y": position[1]},
        "relationships": PROCESSOR_RELATIONSHIPS.get(proc_type, ["success"]),
        "target_ref": name,
        "scheduling": {"schedulingStrategy": "TIMER_DRIVEN", "schedulingPeriod": "0 sec"},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Transformation Processor Builder
# ─────────────────────────────────────────────────────────────────────────────

def build_transformation_processor(
    transformation: Dict[str, Any],
    position: Tuple[int, int],
    reader_service_id: str,
    writer_service_id: str,
) -> Dict[str, Any]:
    """Build a NiFi processor from an Informatica transformation definition."""
    proc_type, description = resolve_processor_type(transformation)
    t_type = transformation.get("type", "")
    t_name = transformation.get("name", "Transform")

    config = {}

    if proc_type == "QueryRecord":
        # Build SQL from translated expressions
        sql_parts = []
        for port in transformation.get("ports", []):
            expr = port.get("expression_translated") or port.get("expression") or port.get("name")
            if expr:
                sql_parts.append(f"{expr} AS {port['name']}")

        if not sql_parts:
            # No expressions — select all
            sql_parts = ["*"]

        sql = "SELECT " + ", ".join(sql_parts) + " FROM FLOWFILE"

        # Handle aggregation
        if "Aggregator" in t_type:
            group_by = [p["name"] for p in transformation.get("ports", []) if p.get("port_type") == "GROUPBY"]
            if group_by:
                sql += " GROUP BY " + ", ".join(group_by)

        # Handle rank
        if "Rank" in t_type:
            rank_by = transformation.get("attributes", {}).get("Rank By", "1")
            rank_order = transformation.get("attributes", {}).get("Order", "ASC")
            sql = f"SELECT *, ROW_NUMBER() OVER (ORDER BY {rank_by} {rank_order}) AS RANK_COL FROM FLOWFILE"

        config = {
            "sql": sql,
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
        }

    elif proc_type == "RouteOnAttribute":
        # Build routing conditions from translated expressions
        routes = {}
        for port in transformation.get("ports", []):
            expr = port.get("expression_translated") or port.get("expression")
            if expr:
                # Convert SQL expression to NiFi EL where possible
                nifi_el = _sql_to_nifi_el(expr, port["name"])
                routes[port["name"]] = nifi_el
        config = {"routes": routes}

        for route_name, condition in routes.items():
            config[f"routing.{route_name}"] = condition

    elif proc_type == "LookupRecord":
        config = {
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
            "Lookup Service": "",  # filled in after controller service creation
        }
        # Add coordinate fields
        coord_fields = []
        for port in transformation.get("ports", []):
            if port.get("port_type") in ("INPUT", "IN"):
                coord_fields.append(port["name"])
        if coord_fields:
            config["coordinate.field"] = coord_fields[0]

    elif proc_type == "MergeRecord":
        merge_strategy = "Bin-Packing Algorithm"
        if "Union" in t_type:
            merge_strategy = "Defragment"
        join_condition = transformation.get("attributes", {}).get("Join Condition", "")
        config = {
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
            "Merge Strategy": merge_strategy,
            "Merge Format": "Record Stream",
            "join_condition_note": join_condition,
        }

    elif proc_type == "SortRecord":
        sort_field = transformation.get("attributes", {}).get("Sort Key Port Name", "ID")
        sort_order = transformation.get("attributes", {}).get("Sort Order", "Ascending...")
        asc = "true" if "scend" in sort_order.lower() and "Desc" not in sort_order else "false"
        config = {
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
            f"sort.0.field": sort_field,
            f"sort.0.order": "ascending" if asc == "true" else "descending",
        }

    elif proc_type == "SplitRecord":
        config = {
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
            "Records Per Split": "1",
        }

    elif proc_type == "ExecuteGroovyScript":
        # Generate stub Groovy script for Java/Spark transformations
        groovy_stub = _generate_groovy_stub(transformation)
        config = {
            "Script Body": groovy_stub,
            "Script Engine": "Groovy",
        }

    elif proc_type == "UpdateRecord":
        updates = {}
        for port in transformation.get("ports", []):
            expr = port.get("expression_translated") or port.get("expression")
            if expr and port.get("port_type") not in ("INPUT", "IN"):
                updates[f"/record/{port['name']}"] = expr
        config = {
            "Record Reader": reader_service_id,
            "Record Writer": writer_service_id,
            **updates
        }

    return {
        "id": gen_id(),
        "type": proc_type,
        "name": t_name,
        "nifi_class": PROCESSOR_CLASS_MAP.get(proc_type, proc_type),
        "config": config,
        "position": {"x": position[0], "y": position[1]},
        "relationships": PROCESSOR_RELATIONSHIPS.get(proc_type, ["success", "failure"]),
        "informatica_type": t_type,
        "description": description,
        "scheduling": {
            "schedulingStrategy": "TIMER_DRIVEN",
            "schedulingPeriod": "0 sec",
            "concurrentTasks": 1,
        },
    }


def _sql_to_nifi_el(sql_expr: str, field_name: str) -> str:
    """
    Best-effort convert a SQL condition to NiFi Expression Language.
    Used for RouteOnAttribute routing conditions.
    """
    import re
    # Simple substitutions
    expr = sql_expr
    # = → :equals()
    expr = re.sub(r"(\w+)\s*=\s*'(.+?)'", r"${\1:equals('\2')}", expr)
    expr = re.sub(r"(\w+)\s*=\s*(\d+)", r"${\1:equals('\2')}", expr)
    # IS NULL / IS NOT NULL
    expr = re.sub(r"(\w+)\s+IS\s+NULL", r"${\1:isNull()}", expr, flags=re.IGNORECASE)
    expr = re.sub(r"(\w+)\s+IS\s+NOT\s+NULL", r"${\1:isNull():not()}", expr, flags=re.IGNORECASE)
    # CASE WHEN → wrap with ${...:equals()}
    if "CASE WHEN" in expr.upper():
        expr = f"${{attr:matches('.*')}}"  # fallback
    # If already NiFi EL, pass through
    if "${" in expr:
        return expr
    # Default: wrap field in EL
    return f"${{{field_name}:equals('{expr}')}}"


def _generate_groovy_stub(transformation: Dict[str, Any]) -> str:
    """Generate a stub Groovy script for unsupported Java/Spark transformations."""
    t_name = transformation.get("name", "java_transform")
    ports_in = [p for p in transformation.get("ports", []) if p.get("port_type") in ("INPUT", "IN")]
    ports_out = [p for p in transformation.get("ports", []) if p.get("port_type") not in ("INPUT", "IN")]

    in_fields = ", ".join(f'"{p["name"]}"' for p in ports_in) or '"input_field"'
    out_fields = "\n    ".join(
        f'record.setValue("{p["name"]}", /* TODO: implement logic */ record.getValue("{p["name"]}"))'
        for p in ports_out
    ) or '// TODO: implement output'

    return f"""// ⚠ AUTO-GENERATED STUB for Java/Spark Transformation: {t_name}
// Original Informatica type: {transformation.get("type", "Unknown")}
// This transformation was NOT automatically migrated (Spark/Java unsupported in NiFi).
// Please implement the logic manually.

import org.apache.nifi.flowfile.FlowFile
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def flowFile = session.get()
if (!flowFile) return

def inputFields = [{in_fields}]

try {{
    flowFile = session.write(flowFile, {{ inputStream, outputStream ->
        def record = new JsonSlurper().parse(inputStream)
        
        // TODO: Implement transformation logic here
        // Input fields: {", ".join(p["name"] for p in ports_in)}
        // Output fields: {", ".join(p["name"] for p in ports_out)}
        
        {out_fields}
        
        outputStream.write(JsonOutput.toJson(record).bytes)
    }} as StreamCallback)
    
    session.transfer(flowFile, REL_SUCCESS)
}} catch (Exception e) {{
    log.error("Error in {t_name}: " + e.getMessage(), e)
    session.transfer(flowFile, REL_FAILURE)
}}
"""


# ─────────────────────────────────────────────────────────────────────────────
# NiFiFlowGenerator
# ─────────────────────────────────────────────────────────────────────────────

class NiFiFlowGenerator:
    """
    Generates a complete NiFi process group definition from:
    - parsed_mapping: output from InformaticaXMLParser.parse()
    - intermediate_model: output from NiFiIntermediateConverter.convert()
    """

    GRID_X_START = 100
    GRID_Y_START = 100
    GRID_X_STEP  = 400
    GRID_Y_STEP  = 200

    def __init__(self, output_dir: str = "output_data"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def _build_controller_services(self) -> Tuple[List[Dict], str, str, str]:
        """Create standard controller services needed by most processors."""
        reader_id = gen_id()
        writer_id = gen_id()
        db_id = gen_id()
        csv_reader_id = gen_id()

        services = [
            {
                "id": reader_id,
                "type": "AvroReader",
                "nifi_class": CONTROLLER_SERVICE_CLASS_MAP["AvroReader"],
                "name": "AvroReader",
                "state": "ENABLED",
                "config": {},
            },
            {
                "id": writer_id,
                "type": "JsonRecordSetWriter",
                "nifi_class": CONTROLLER_SERVICE_CLASS_MAP["JsonRecordSetWriter"],
                "name": "JsonRecordSetWriter",
                "state": "ENABLED",
                "config": {
                    "Schema Write Strategy": "no-schema",
                    "Date Format": "yyyy-MM-dd",
                    "Timestamp Format": "yyyy-MM-dd HH:mm:ss",
                },
            },
            {
                "id": csv_reader_id,
                "type": "CSVReader",
                "nifi_class": CONTROLLER_SERVICE_CLASS_MAP["CSVReader"],
                "name": "CSVReader",
                "state": "ENABLED",
                "config": {
                    "schema-access-strategy": "csv-header-derived",
                    "Skip Header Line": "false",
                },
            },
            {
                "id": db_id,
                "type": "DBCPConnectionPool",
                "nifi_class": CONTROLLER_SERVICE_CLASS_MAP["DBCPConnectionPool"],
                "name": "DBCPConnectionPool",
                "state": "ENABLED",
                "config": {
                    "Database Connection URL": "jdbc:postgresql://localhost:5432/postgres",
                    "Database Driver Class Name": "org.postgresql.Driver",
                    "Database Driver Location(s)": "/opt/nifi/nifi-current/lib/postgresql.jar",
                    "Database User": "postgres",
                    "Password": "password",
                    "Max Wait Time": "500 millis",
                    "Max Total Connections": "8",
                },
            },
        ]
        return services, reader_id, writer_id, db_id

    def generate(
        self,
        parsed_mapping: Dict[str, Any],
        intermediate_model: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a complete NiFi flow definition.

        Args:
            parsed_mapping: Output from InformaticaXMLParser.parse()
            intermediate_model: Optional output from NiFiIntermediateConverter.convert()

        Returns:
            NiFi flow definition dict
        """
        flow_name = parsed_mapping.get("mapping_name", "UnknownFlow")
        logger.info(f"Generating NiFi flow for: {flow_name}")

        # Build controller services
        controller_services, reader_id, writer_id, db_id = self._build_controller_services()

        processors = []
        connections = []
        instance_to_proc_id: Dict[str, str] = {}

        col = 0  # grid column counter

        # ── Build source processors ───────────────────────────────────────────
        source_proc_ids = []
        for source in parsed_mapping.get("sources", []):
            x = self.GRID_X_START + (col * self.GRID_X_STEP)
            y = self.GRID_Y_START
            proc = build_source_processor(source, (x, y))
            processors.append(proc)
            source_proc_ids.append(proc["id"])
            instance_to_proc_id[source["name"]] = proc["id"]

        last_source_id = source_proc_ids[-1] if source_proc_ids else None

        # ── Build transformation processors (in execution order) ─────────────
        col = len(source_proc_ids)
        transformation_map = {t["name"]: t for t in parsed_mapping.get("transformations", [])}

        execution_order = parsed_mapping.get("execution_order", [])
        if not execution_order:
            execution_order = list(transformation_map.keys())

        row = 0
        for instance_name in execution_order:
            trans = transformation_map.get(instance_name)
            if not trans:
                continue

            x = self.GRID_X_START + (col * self.GRID_X_STEP)
            y = self.GRID_Y_START + (row * self.GRID_Y_STEP)

            proc = build_transformation_processor(trans, (x, y), reader_id, writer_id)
            processors.append(proc)
            instance_to_proc_id[instance_name] = proc["id"]

            col += 1
            if col > 5:  # wrap to next row after 5 columns
                col = len(source_proc_ids)
                row += 1

        # ── Build target processors ───────────────────────────────────────────
        target_proc_ids = []
        for i, target in enumerate(parsed_mapping.get("targets", [])):
            x = self.GRID_X_START + (col * self.GRID_X_STEP)
            y = self.GRID_Y_START + (i * self.GRID_Y_STEP)
            proc = build_target_processor(target, (x, y), reader_id)
            processors.append(proc)
            target_proc_ids.append(proc["id"])
            instance_to_proc_id[target["name"]] = proc["id"]

        # ── Build connections from Informatica connectors ─────────────────────
        # First pass: register "intermediate" source instances not in
        # instance_to_proc_id (e.g. DUAL_core / DUAL_intg → Read_DUAL).
        # Priority: source processors > transformations. Never map to the
        # connector's OWN target (prevents self-loops like DUAL_core→SQ_DUAL_core
        # being resolved as SQ_DUAL_core→SQ_DUAL_core).
        for connector in parsed_mapping.get("connectors", []):
            src_name = connector["from_instance"]
            tgt_name = connector["to_instance"]
            if src_name in instance_to_proc_id:
                continue
            sname_up = src_name.upper()
            tgt_id_to_skip = instance_to_proc_id.get(tgt_name)  # avoid self-loop

            # Step 1: match to a SOURCE processor (by source name or source_ref)
            matched = None
            for proc in processors:
                if proc["id"] in source_proc_ids and proc["id"] != tgt_id_to_skip:
                    pname_up = proc["name"].upper()
                    # e.g. DUAL_CORE contains DUAL, Read_DUAL also contains DUAL
                    src_base = sname_up.split("_")[0]   # DUAL from DUAL_CORE
                    if src_base and src_base in pname_up:
                        matched = proc
                        break

            # Step 2: fall back to any processor by name substring (skip self)
            if not matched:
                for proc in processors:
                    if proc["id"] == tgt_id_to_skip:
                        continue
                    pname_up = proc["name"].upper()
                    if sname_up in pname_up or pname_up in sname_up:
                        matched = proc
                        break

            if matched:
                instance_to_proc_id[src_name] = matched["id"]
                logger.debug(f"  Alias resolved: {src_name} → {matched['name']}")

        used_connections = set()
        next_x, next_y = 2500.0, 100.0   # position for auto-created target procs

        for connector in parsed_mapping.get("connectors", []):
            src_name = connector["from_instance"]
            tgt_name = connector["to_instance"]

            src_id = instance_to_proc_id.get(src_name)
            tgt_id = instance_to_proc_id.get(tgt_name)

            # Auto-create a PutDatabaseRecord for unknown target instances
            if src_id and not tgt_id:
                proc_name = f"Write_{tgt_name}"
                new_proc = {
                    "id": gen_id(),
                    "name": proc_name,
                    "type": "PutDatabaseRecord",
                    "nifi_class": "org.apache.nifi.processors.standard.PutDatabaseRecord",
                    "position": {"x": next_x, "y": next_y},
                    "config": {
                        "Table Name": tgt_name,
                        "Statement Type": "UPDATE",
                    },
                    "relationships": ["success", "failure", "retry"],
                    "scheduling": {"schedulingStrategy": "TIMER_DRIVEN", "schedulingPeriod": "0 sec"},
                    "description": f"Auto-created target for Informatica instance: {tgt_name}",
                }
                processors.append(new_proc)
                target_proc_ids.append(new_proc["id"])
                instance_to_proc_id[tgt_name] = new_proc["id"]
                tgt_id = new_proc["id"]
                next_y += 200.0
                logger.info(f"  Auto-created missing target processor: {proc_name}")

            if src_id and tgt_id and (src_id, tgt_id) not in used_connections:
                used_connections.add((src_id, tgt_id))
                src_proc = next((p for p in processors if p["id"] == src_id), None)
                src_type = src_proc["type"] if src_proc else "QueryRecord"
                rels = PROCESSOR_RELATIONSHIPS.get(src_type, ["success"])
                primary_rel = rels[0] if rels else "success"

                connections.append({
                    "id": gen_id(),
                    "source_id": src_id,
                    "target_id": tgt_id,
                    "selected_relationships": [primary_rel],
                    "back_pressure_object_threshold": "10000",
                    "back_pressure_data_size_threshold": "1 GB",
                })

        # ── Connect sources to first transformation if no connector specified ─
        if last_source_id and processors and not connections:
            trans_procs = [p for p in processors if p["id"] not in source_proc_ids + target_proc_ids]
            if trans_procs:
                connections.append({
                    "id": gen_id(),
                    "source_id": last_source_id,
                    "target_id": trans_procs[0]["id"],
                    "selected_relationships": ["success"],
                    "back_pressure_object_threshold": "10000",
                    "back_pressure_data_size_threshold": "1 GB",
                })

        # ── Heal orphaned source → SQ links (implicit in Informatica XML) ─────
        # In Informatica, the Source→SourceQualifier binding is implicit and
        # not encoded as a <CONNECTOR> element. We detect orphaned source
        # processors and match them to orphaned transformation processors by
        # checking whether the source's table name appears in the SQ processor name.
        connected_sources = {c["source_id"] for c in connections}
        connected_targets = {c["target_id"] for c in connections}

        orphan_sources = [
            p for p in processors
            if p["id"] in source_proc_ids and p["id"] not in connected_sources
        ]
        orphan_transforms = [
            p for p in processors
            if p["id"] not in source_proc_ids
            and p["id"] not in target_proc_ids
            and p["id"] not in connected_targets
        ]

        # Match orphaned sources → orphaned transforms by table name substring
        healed = set()
        for src_proc in orphan_sources:
            table_name = (src_proc.get("source_ref") or src_proc.get("name", "")).upper()
            matched = [
                t for t in orphan_transforms
                if table_name and table_name in t["name"].upper()
                and t["id"] not in healed
            ]
            for tgt_proc in matched:
                if (src_proc["id"], tgt_proc["id"]) not in used_connections:
                    used_connections.add((src_proc["id"], tgt_proc["id"]))
                    connections.append({
                        "id": gen_id(),
                        "source_id": src_proc["id"],
                        "target_id": tgt_proc["id"],
                        "selected_relationships": ["success"],
                        "back_pressure_object_threshold": "10000",
                        "back_pressure_data_size_threshold": "1 GB",
                    })
                    healed.add(tgt_proc["id"])
                    logger.info(f"  Auto-linked: {src_proc['name']} → {tgt_proc['name']}")

        # Link any remaining orphaned transforms (still no inbound) to the
        # last connected transformation so nothing is stranded
        connected_targets = {c["target_id"] for c in connections}
        still_orphan = [
            p for p in processors
            if p["id"] not in source_proc_ids
            and p["id"] not in target_proc_ids
            and p["id"] not in connected_targets
        ]
        if still_orphan:
            # Find the last processor that HAS an outbound connection
            connected_sources = {c["source_id"] for c in connections}
            anchor_procs = [
                p for p in processors
                if p["id"] in connected_sources
                and p["id"] not in source_proc_ids
                and p["id"] not in target_proc_ids
            ]
            if anchor_procs:
                anchor_id = anchor_procs[-1]["id"]
                for orph in still_orphan:
                    if (anchor_id, orph["id"]) not in used_connections:
                        used_connections.add((anchor_id, orph["id"]))
                        connections.append({
                            "id": gen_id(),
                            "source_id": anchor_id,
                            "target_id": orph["id"],
                            "selected_relationships": ["success"],
                            "back_pressure_object_threshold": "10000",
                            "back_pressure_data_size_threshold": "1 GB",
                        })
                        logger.info(f"  Fallback-linked orphan: {anchor_procs[-1]['name']} → {orph['name']}")

        # ── Connect last transformation to truly unreachable targets only ──────
        # Only add a fallback connection if the target has NO inbound connections
        # from ANY processor. Targets already reachable (e.g. Write_PO_WD_COST_CENTER
        # from EXP_FILES) must NOT receive a second, wrong-relationship connection
        # from the last transformation (e.g. FIL_NO_INACTIVATIONS via 'success',
        # which doesn't exist on RouteOnAttribute → dotted lines).
        if target_proc_ids and processors:
            all_conn_targets = {c["target_id"] for c in connections}
            trans_procs = [p for p in processors if p["id"] not in source_proc_ids + target_proc_ids]
            if trans_procs:
                last_trans = trans_procs[-1]
                last_trans_id = last_trans["id"]
                last_rels = PROCESSOR_RELATIONSHIPS.get(last_trans["type"], ["success"])
                last_primary_rel = last_rels[0] if last_rels else "success"

                for tgt_id in target_proc_ids:
                    # Skip targets that already have at least one inbound connection
                    if tgt_id in all_conn_targets:
                        continue
                    if (last_trans_id, tgt_id) not in used_connections:
                        used_connections.add((last_trans_id, tgt_id))
                        connections.append({
                            "id": gen_id(),
                            "source_id": last_trans_id,
                            "target_id": tgt_id,
                            "selected_relationships": [last_primary_rel],
                            "back_pressure_object_threshold": "10000",
                            "back_pressure_data_size_threshold": "1 GB",
                        })
                        logger.info(
                            f"  Fallback target link: {last_trans['name']} "
                            f"--[{last_primary_rel}]--> "
                            f"{next((p['name'] for p in processors if p['id']==tgt_id), tgt_id)}"
                        )


        # ── Compute auto-terminate relationships ─────────────────────────────
        # A relationship should be auto-terminated only if it is NOT used by
        # any outgoing connection from that processor. Marking it both
        # auto-terminated AND connected causes dotted lines in the NiFi UI.
        used_rels_by_proc: Dict[str, set] = {p["id"]: set() for p in processors}
        for conn in connections:
            src = conn["source_id"]
            if src in used_rels_by_proc:
                used_rels_by_proc[src].update(conn.get("selected_relationships", []))

        for proc in processors:
            all_rels = proc.get("relationships", [])
            used = used_rels_by_proc.get(proc["id"], set())
            proc["auto_terminate_relationships"] = [r for r in all_rels if r not in used]

        # ── Assemble complete flow definition ─────────────────────────────────
        flow = {
            "flow_id": gen_id(),
            "flow_name": flow_name,
            "revision": {"version": 0},
            "component": {
                "id": gen_id(),
                "name": flow_name,
                "position": {"x": 0.0, "y": 0.0},
                "comments": f"Auto-generated from Informatica mapping: {flow_name}",
            },
            "controller_services": controller_services,
            "processors": processors,
            "connections": connections,
            "metadata": {
                "source_count": len(parsed_mapping.get("sources", [])),
                "target_count": len(parsed_mapping.get("targets", [])),
                "transformation_count": len(parsed_mapping.get("transformations", [])),
                "processor_count": len(processors),
                "connection_count": len(connections),
                "complexity": parsed_mapping.get("complexity", "UNKNOWN"),
                "has_spark": parsed_mapping.get("spark_report", {}).get("has_spark", False),
            },
        }

        # Generate processor summary for logging
        type_summary = {}
        for p in processors:
            type_summary[p["type"]] = type_summary.get(p["type"], 0) + 1

        logger.info(f"Flow '{flow_name}' generated: {len(processors)} processors, {len(connections)} connections")
        logger.info(f"Processor breakdown: {type_summary}")

        return flow

    def to_nifi_template_xml(self, flow: Dict[str, Any]) -> str:
        """
        Convert a flow dict to NiFi's importable XML Template format.
        Can be imported via NiFi UI → Operate panel → Upload Template.
        Follows the NiFi 1.x template XML schema including required
        parentGroupId and groupId fields.
        """
        from xml.etree.ElementTree import Element, SubElement, tostring
        from xml.dom import minidom

        # Generate a stable group ID that all processors/connections reference
        group_id = flow.get("flow_id", gen_id())

        template = Element("template")
        SubElement(template, "description").text = f"Auto-generated from Informatica: {flow['flow_name']}"
        SubElement(template, "name").text = flow["flow_name"]
        SubElement(template, "timestamp").text = "00:00:00 UTC"

        snippet = SubElement(template, "snippet")

        # ── Controller Services ───────────────────────────────────────────────
        for svc in flow.get("controller_services", []):
            cs = SubElement(snippet, "controllerServices")
            SubElement(cs, "id").text = svc["id"]
            SubElement(cs, "parentGroupId").text = group_id   # ← required
            SubElement(cs, "name").text = svc.get("name", svc["type"])
            SubElement(cs, "type").text = svc.get("nifi_class", svc["type"])
            SubElement(cs, "state").text = svc.get("state", "ENABLED")
            props = SubElement(cs, "properties")
            for k, v in svc.get("config", {}).items():
                entry = SubElement(props, "entry")
                SubElement(entry, "key").text = str(k)
                SubElement(entry, "value").text = str(v)

        # ── Processors ────────────────────────────────────────────────────────
        for proc in flow.get("processors", []):
            p = SubElement(snippet, "processors")
            SubElement(p, "id").text = proc["id"]
            SubElement(p, "parentGroupId").text = group_id    # ← required

            pos = SubElement(p, "position")
            position = proc.get("position", {"x": 0, "y": 0})
            SubElement(pos, "x").text = str(float(position.get("x", 0)))
            SubElement(pos, "y").text = str(float(position.get("y", 0)))

            # config block — element order matters to NiFi's XML parser
            cfg = SubElement(p, "config")
            SubElement(cfg, "bulletinLevel").text = "WARN"
            SubElement(cfg, "comments").text = proc.get("description", "")
            SubElement(cfg, "concurrentlySchedulableTaskCount").text = "1"
            SubElement(cfg, "executionNode").text = "ALL"
            SubElement(cfg, "lossTolerant").text = "false"
            SubElement(cfg, "penaltyDuration").text = "30 sec"

            props = SubElement(cfg, "properties")
            for k, v in proc.get("config", {}).items():
                if k in ("routes",):
                    continue
                entry = SubElement(props, "entry")
                SubElement(entry, "key").text = str(k)
                SubElement(entry, "value").text = str(v) if v is not None else ""

            scheduling = proc.get("scheduling", {})
            SubElement(cfg, "runDurationMillis").text = "0"
            SubElement(cfg, "schedulingPeriod").text = scheduling.get("schedulingPeriod", "0 sec")
            SubElement(cfg, "schedulingStrategy").text = scheduling.get("schedulingStrategy", "TIMER_DRIVEN")
            SubElement(cfg, "yieldDuration").text = "1 sec"

            SubElement(p, "name").text = proc["name"]

            # relationships block (empty — NiFi will populate from processor type)
            SubElement(p, "relationships")
            SubElement(p, "style")
            SubElement(p, "type").text = proc.get("nifi_class", proc["type"])

        # ── Connections ───────────────────────────────────────────────────────
        for conn in flow.get("connections", []):
            c = SubElement(snippet, "connections")
            SubElement(c, "id").text = conn["id"]
            SubElement(c, "backPressureDataSizeThreshold").text = conn.get("back_pressure_data_size_threshold", "1 GB")
            SubElement(c, "backPressureObjectThreshold").text = conn.get("back_pressure_object_threshold", "10000")

            dst = SubElement(c, "destination")
            SubElement(dst, "groupId").text = group_id        # ← required
            SubElement(dst, "id").text = conn["target_id"]
            SubElement(dst, "type").text = "PROCESSOR"

            SubElement(c, "labelIndex").text = "1"
            SubElement(c, "name")

            for rel in conn.get("selected_relationships", ["success"]):
                SubElement(c, "selectedRelationships").text = rel

            src = SubElement(c, "source")
            SubElement(src, "groupId").text = group_id        # ← required
            SubElement(src, "id").text = conn["source_id"]
            SubElement(src, "type").text = "PROCESSOR"

            SubElement(c, "zIndex").text = "0"

        # ── Serialise without minidom (minidom can introduce BOM/encoding issues) ─
        try:
            import xml.etree.ElementTree as _ET
            _ET.indent(template, space="  ")   # Python 3.9+ pretty-print in-place
        except AttributeError:
            pass                               # Python < 3.9 — unindented, still valid

        raw = tostring(template, encoding="unicode")

        # Prepend the XML declaration as a plain string — no BOM, no minidom quirks
        return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + raw



    def export(
        self,
        parsed_mapping: Dict[str, Any],
        intermediate_model: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate and save the flow JSON + NiFi XML template to disk. Returns the JSON file path."""
        flow = self.generate(parsed_mapping, intermediate_model)
        flow_name = flow["flow_name"]

        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in flow_name)
        json_path = os.path.join(self.output_dir, f"{safe_name}_nifi_flow.json")
        xml_path  = os.path.join(self.output_dir, f"{safe_name}_nifi_template.xml")

        with open(json_path, "w") as f:
            json.dump(flow, f, indent=2)

        xml_content = self.to_nifi_template_xml(flow)
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)

        logger.info(f"Flow JSON exported to: {json_path}")
        logger.info(f"NiFi XML template exported to: {xml_path}  ← Import this in NiFi UI")
        return json_path



# ─────────────────────────────────────────────────────────────────────────────
# LangGraph Node
# ─────────────────────────────────────────────────────────────────────────────

def flow_export_node(state: dict) -> dict:
    """LangGraph node: generates and exports the complete NiFi flow JSON."""
    generator = NiFiFlowGenerator(output_dir="output_data")

    parsed = state.get("parsed_mapping", {})

    # Attach spark_report to parsed for metadata
    if "spark_report" in state:
        parsed["spark_report"] = state["spark_report"]

    export_path = generator.export(parsed, state.get("nifi_intermediate"))
    state["export_path"] = export_path

    # Also store in state for downstream use
    with open(export_path) as f:
        import json
        state["nifi_flow_json"] = json.load(f)

    logger.info(f"NiFi flow exported: {export_path}")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Quick test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    sample_parsed = {
        "mapping_name": "m_customer_dim_load",
        "complexity": "MEDIUM",
        "sources": [
            {"name": "src_customers", "database_type": "Oracle", "owner": "DW", "fields": []},
        ],
        "targets": [
            {"name": "tgt_customer_dim", "database_type": "Oracle", "owner": "DW", "fields": []},
        ],
        "transformations": [
            {
                "name": "exp_calculations",
                "type": "Expression",
                "description": None,
                "ports": [
                    {"name": "FULL_NAME", "expression_translated": "CONCAT(FIRST_NAME, ' ', LAST_NAME)", "expression": "FIRST_NAME || ' ' || LAST_NAME", "datatype": "string", "port_type": "OUTPUT"},
                    {"name": "LOAD_DATE", "expression_translated": "NOW()", "expression": "SYSDATE", "datatype": "date/time", "port_type": "OUTPUT"},
                ],
                "attributes": {},
            },
            {
                "name": "rtr_active_only",
                "type": "Router",
                "description": None,
                "ports": [
                    {"name": "active", "expression_translated": "CASE WHEN STATUS = 'A' THEN 1 ELSE 0 END", "expression": "IIF(STATUS='A', 1, 0)", "datatype": "integer", "port_type": "OUTPUT"},
                ],
                "attributes": {},
            },
        ],
        "connectors": [
            {"from_instance": "src_customers", "to_instance": "exp_calculations"},
            {"from_instance": "exp_calculations", "to_instance": "rtr_active_only"},
            {"from_instance": "rtr_active_only", "to_instance": "tgt_customer_dim"},
        ],
        "execution_order": ["exp_calculations", "rtr_active_only"],
        "instances": [],
    }

    gen = NiFiFlowGenerator(output_dir="/tmp/nifi_test")
    path = gen.export(sample_parsed)
    print(f"Generated: {path}")

    with open(path) as f:
        flow = json.load(f)
    print(f"Processors: {len(flow['processors'])}")
    print(f"Connections: {len(flow['connections'])}")
    for p in flow["processors"]:
        print(f"  [{p['type']:25s}] {p['name']} — {p.get('description', p.get('informatica_type', ''))}")
