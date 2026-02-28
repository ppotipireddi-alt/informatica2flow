# ---------------------------------------------------------
# Enhanced Validation Engine
# ---------------------------------------------------------
import logging
from typing import Dict, Any, List

logger = logging.getLogger("ValidationEngine")


def validate_intermediate(intermediate: Dict[str, Any]) -> Dict[str, Any]:
    """Validate an intermediate NiFi model for completeness and correctness."""
    processors = intermediate.get("processors", [])
    connections = intermediate.get("connections", [])
    services = intermediate.get("controller_services", [])

    errors = []
    warnings = []

    proc_id_set = {p["id"] for p in processors}

    # 1. QueryRecord must have SQL
    for p in processors:
        if p["type"] == "QueryRecord" and not p.get("config", {}).get("sql"):
            errors.append(f"Processor '{p['name']}' (QueryRecord) has no SQL configured")

    # 2. Connection IDs must reference real processors
    for conn in connections:
        if conn.get("source") and conn["source"] not in proc_id_set:
            errors.append(f"Connection references unknown source processor: {conn['source']}")
        if conn.get("target") and conn["target"] not in proc_id_set:
            errors.append(f"Connection references unknown target processor: {conn['target']}")

    # 3. LookupRecord needs a lookup service
    for p in processors:
        if p["type"] == "LookupRecord" and not p.get("config", {}).get("lookup_service"):
            warnings.append(f"Processor '{p['name']}' (LookupRecord) missing lookup_service — configure manually")

    # 4. Spark/Java stubs
    for p in processors:
        if p["type"] == "ExecuteGroovyScript":
            warnings.append(f"Processor '{p['name']}' is a Groovy stub — manual implementation required")

    # 5. Orphaned processors
    connected_ids = {c["source"] for c in connections} | {c["target"] for c in connections}
    for p in processors:
        if p["id"] not in connected_ids and len(processors) > 1:
            warnings.append(f"Processor '{p['name']}' has no connections — may be orphaned")

    # 6. Cyclic dependency check (simple)
    from collections import defaultdict, deque
    graph = defaultdict(list)
    for conn in connections:
        if conn.get("source") and conn.get("target"):
            graph[conn["source"]].append(conn["target"])
    visited, rec_stack = set(), set()

    def has_cycle(node):
        visited.add(node)
        rec_stack.add(node)
        for nb in graph[node]:
            if nb not in visited:
                if has_cycle(nb):
                    return True
            elif nb in rec_stack:
                return True
        rec_stack.discard(node)
        return False

    for p in processors:
        if p["id"] not in visited:
            if has_cycle(p["id"]):
                errors.append("Cyclic dependency detected in processor connections")
                break

    report = {
        "empty_sql": any("no SQL" in e for e in errors),
        "missing_processors": False,
        "errors": errors,
        "warnings": warnings,
        "valid": len(errors) == 0,
        "processor_count": len(processors),
        "connection_count": len(connections),
    }

    if errors:
        logger.warning(f"Validation errors: {errors}")
    if warnings:
        logger.info(f"Validation warnings: {warnings}")

    return report


# ---------------------------------------------------------
# Validation Node (LangGraph)
# ---------------------------------------------------------
from migration_state import MigrationState


def validation_node(state: MigrationState):
    """Validate the intermediate model, falling back to flow JSON if available."""
    intermediate = state.get("nifi_intermediate") or state.get("nifi_flow_json")
    if not intermediate:
        state["validation_report"] = {"errors": ["No intermediate model or flow JSON found"], "valid": False}
        return state

    report = validate_intermediate(intermediate)
    state["validation_report"] = report
    return state