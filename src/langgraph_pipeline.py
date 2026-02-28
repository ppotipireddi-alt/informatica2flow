"""
langgraph_pipeline.py
---------------------
LangGraph orchestration pipeline for Informatica → NiFi migration.

Pipeline DAG:
  parse
    ↓
  spark_detection ── [has_spark] ──→ human_review
    ↓ [no spark]                          ↓
  expression_translation                 END
    ↓
  datatype_mapping
    ↓
  convert (NiFiIntermediateConverter)
    ↓
  validate
    ↓
  confidence
    ↓
  flow_export ── [low confidence] ──→ human_review
    ↓ [high confidence]                   ↓
  deploy                                 END
    ↓
  END
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LangGraphPipeline")

# Ensure src/ is on the path when run from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from migration_state import MigrationState


# ─────────────────────────────────────────────────────────────────────────────
# Node 1: Parse
# ─────────────────────────────────────────────────────────────────────────────

from informatica_parser import InformaticaXMLParser


def parser_node(state: MigrationState) -> MigrationState:
    """Parse the Informatica XML file."""
    parser = InformaticaXMLParser(state["xml_path"])
    parsed = parser.parse()

    state["parsed_mapping"] = parsed
    state["complexity"] = parsed["complexity"]
    state["confidence_scores"] = []
    state["review_required"] = False

    logger.info(f"✅ Parsed: {parsed['mapping_name']} | Complexity: {parsed['complexity']}")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 2: Spark Detection
# ─────────────────────────────────────────────────────────────────────────────

from spark_detector import SparkDetector, spark_detection_node


# ─────────────────────────────────────────────────────────────────────────────
# Node 3: Expression Translation
# ─────────────────────────────────────────────────────────────────────────────

from expression_translator import ExpressionTranslator, expression_translation_node


# ─────────────────────────────────────────────────────────────────────────────
# Node 4: Datatype Mapping
# ─────────────────────────────────────────────────────────────────────────────

from datatype_mapper import DataTypeMapper, datatype_mapping_node


# ─────────────────────────────────────────────────────────────────────────────
# Node 5: Conversion (NiFi Intermediate Model)
# ─────────────────────────────────────────────────────────────────────────────

from nifi_intermediate_converter import NiFiIntermediateConverter
from rule_memory_engine import RuleMemoryEngine
from config import DB_CONFIG


def conversion_node(state: MigrationState) -> MigrationState:
    """Convert parsed mapping to NiFi intermediate model."""
    try:
        rule_engine = RuleMemoryEngine(DB_CONFIG)
    except Exception as e:
        logger.warning(f"Rule engine unavailable: {e} — using regex+LLM only")
        rule_engine = None

    converter = NiFiIntermediateConverter(
        parsed_mapping=state["parsed_mapping"],
        rule_engine=rule_engine,
    )

    intermediate = converter.convert()
    state["nifi_intermediate"] = intermediate

    scores = getattr(converter, "confidence_scores", [])
    existing = state.get("confidence_scores", [])
    state["confidence_scores"] = existing + (scores if scores else [0.8])

    logger.info(f"✅ Converted: {len(intermediate.get('processors', []))} processors, {len(intermediate.get('connections', []))} connections")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 6: Validation
# ─────────────────────────────────────────────────────────────────────────────

from validation_engine import validation_node


# ─────────────────────────────────────────────────────────────────────────────
# Node 7: Confidence Scoring
# ─────────────────────────────────────────────────────────────────────────────

from confidence_engine import confidence_node


# ─────────────────────────────────────────────────────────────────────────────
# Node 8: Complete NiFi Flow JSON Export
# ─────────────────────────────────────────────────────────────────────────────

from nifi_flow_generator import NiFiFlowGenerator, flow_export_node


def _flow_export_node(state: MigrationState) -> MigrationState:
    """Generate and export the complete NiFi flow JSON."""
    generator = NiFiFlowGenerator(output_dir="output_data")
    parsed = state.get("parsed_mapping", {})
    if "spark_report" in state:
        parsed["spark_report"] = state["spark_report"]

    export_path = generator.export(parsed, state.get("nifi_intermediate"))
    state["export_path"] = export_path

    import json
    with open(export_path) as f:
        state["nifi_flow_json"] = json.load(f)

    logger.info(f"✅ Flow exported: {export_path}")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 9: Deployment
# ─────────────────────────────────────────────────────────────────────────────

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_orig_request = requests.Session.request


def _ssl_patched_request(self, method, url, **kwargs):
    kwargs.setdefault("verify", False)
    return _orig_request(self, method, url, **kwargs)


requests.Session.request = _ssl_patched_request

from nifi_rest_deployer import NiFiRestDeployer
from config import NIFI_CONFIG


def _get_nifi_token(nifi_url: str, username: str, password: str) -> str:
    token_url = f"{nifi_url.rstrip('/')}/nifi-api/access/token"
    r = requests.post(
        token_url,
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        verify=False,
    )
    r.raise_for_status()
    return r.text.strip()


class _PatchedNiFiDeployer(NiFiRestDeployer):
    def create_connection(self, group_id, source_id, target_id):
        url = f"{self.base_url}/nifi-api/process-groups/{group_id}/connections"
        payload = {
            "revision": {"version": 0},
            "component": {
                "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
                "destination": {"id": target_id, "groupId": group_id, "type": "PROCESSOR"},
                "selectedRelationships": ["success"],
            },
        }
        r = self.session.post(url, json=payload)
        r.raise_for_status()


def deploy_node(state: MigrationState) -> MigrationState:
    """Deploy the generated flow to NiFi via REST."""
    nifi_url = NIFI_CONFIG["url"]
    username = NIFI_CONFIG["username"]
    password = NIFI_CONFIG["password"]

    try:
        token = _get_nifi_token(nifi_url, username, password)
        deployer = _PatchedNiFiDeployer(nifi_url)
        deployer.session.headers.update({"Authorization": f"Bearer {token}"})

        # Prefer full flow JSON if available, else intermediate
        model = state.get("nifi_flow_json") or state.get("nifi_intermediate")
        if not model:
            logger.error("No flow model available for deployment")
            return state

        group_id = deployer.deploy(
            parent_group_id="root",
            intermediate_model=model,
        )
        state["deployed_group_id"] = group_id
        logger.info(f"✅ Deployed to NiFi group: {group_id}")

    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        state["deployed_group_id"] = ""

    return state


# ─────────────────────────────────────────────────────────────────────────────
# Node 10: Human Review
# ─────────────────────────────────────────────────────────────────────────────

import json


def human_review_node(state: MigrationState) -> MigrationState:
    """Route to human review queue (save review packet to disk)."""
    mapping_name = state.get("parsed_mapping", {}).get("mapping_name", "unknown")
    logger.warning(f"⚠️  Manual review required for: {mapping_name}")

    review_dir = "review_queue"
    os.makedirs(review_dir, exist_ok=True)
    review_path = os.path.join(review_dir, f"{mapping_name}_review.json")

    review_packet = {
        "mapping_name": mapping_name,
        "complexity": state.get("complexity"),
        "overall_confidence": state.get("overall_confidence", 0),
        "spark_report": state.get("spark_report", {}),
        "expression_translations": state.get("expression_translations", []),
        "validation_report": state.get("validation_report", {}),
        "export_path": state.get("export_path", ""),
    }

    with open(review_path, "w") as f:
        json.dump(review_packet, f, indent=2)

    logger.info(f"Review packet saved: {review_path}")
    return state


# ─────────────────────────────────────────────────────────────────────────────
# Routing Logic
# ─────────────────────────────────────────────────────────────────────────────

def route_after_spark(state: MigrationState) -> str:
    """If Spark detected, go to human review. Otherwise continue."""
    spark = state.get("spark_report", {})
    if spark.get("has_spark"):
        logger.warning("Routing to human review: Spark dependencies detected")
        return "review"
    return "translate_expressions"


def route_after_confidence(state: MigrationState) -> str:
    """If confidence too low, go to human review. Otherwise export."""
    if state.get("review_required"):
        return "review"
    return "export_flow"


# ─────────────────────────────────────────────────────────────────────────────
# Build Graph
# ─────────────────────────────────────────────────────────────────────────────

from langgraph.graph import StateGraph, END


def build_graph(skip_deploy: bool = False) -> object:
    """Build and compile the LangGraph migration pipeline."""
    workflow = StateGraph(MigrationState)

    # Register all nodes
    workflow.add_node("parse",               parser_node)
    workflow.add_node("detect_spark",        spark_detection_node)
    workflow.add_node("translate_expressions", expression_translation_node)
    workflow.add_node("map_datatypes",       datatype_mapping_node)
    workflow.add_node("convert",             conversion_node)
    workflow.add_node("validate",            validation_node)
    workflow.add_node("confidence",          confidence_node)
    workflow.add_node("export_flow",         _flow_export_node)
    workflow.add_node("deploy",              deploy_node)
    workflow.add_node("review",              human_review_node)

    # Sequential edges
    workflow.set_entry_point("parse")
    workflow.add_edge("parse", "detect_spark")

    # Conditional: Spark detected → review, else → translations
    workflow.add_conditional_edges("detect_spark", route_after_spark)

    workflow.add_edge("translate_expressions", "map_datatypes")
    workflow.add_edge("map_datatypes", "convert")
    workflow.add_edge("convert", "validate")
    workflow.add_edge("validate", "confidence")

    # Conditional: low confidence → review, else → export
    workflow.add_conditional_edges("confidence", route_after_confidence)

    workflow.add_edge("export_flow", "deploy" if not skip_deploy else END)
    workflow.add_edge("deploy", END)
    workflow.add_edge("review", END)

    return workflow.compile()


# ─────────────────────────────────────────────────────────────────────────────
# Single Migration Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def run_migration(xml_path: str, skip_deploy: bool = False) -> MigrationState:
    """Run the full migration pipeline for a single XML file."""
    graph = build_graph(skip_deploy=skip_deploy)
    initial_state: MigrationState = {"xml_path": xml_path}
    result = graph.invoke(initial_state)

    mapping_name = result.get("parsed_mapping", {}).get("mapping_name", "unknown")
    confidence = result.get("overall_confidence", 0)
    export_path = result.get("export_path", "N/A")
    group_id = result.get("deployed_group_id", "N/A")

    print(f"\n{'='*60}")
    print(f"Migration Report: {mapping_name}")
    print(f"{'='*60}")
    print(f"  Complexity:      {result.get('complexity', 'N/A')}")
    print(f"  Confidence:      {confidence:.2%}")
    print(f"  Review Required: {result.get('review_required', False)}")
    print(f"  Flow Exported:   {export_path}")
    if not skip_deploy:
        print(f"  NiFi Group ID:   {group_id}")

    spark = result.get("spark_report", {})
    if spark.get("has_spark"):
        print(f"\n  ⚠️  Spark Detected: {spark.get('unsupported_count', 0)} unsupported component(s)")

    trans = result.get("expression_translations", [])
    if trans:
        print(f"\n  Expressions Translated: {len(trans)}")
        for t in trans[:5]:
            print(f"    [{t['method']:15s}] ({t['confidence']:.2f}) {t['original']!r}")
            print(f"                       → {t['translated']!r}")

    print(f"{'='*60}\n")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Batch Migration
# ─────────────────────────────────────────────────────────────────────────────

def run_batch(directory: str, skip_deploy: bool = True) -> list:
    """Run migration for all XML files in a directory."""
    results = []
    for file in os.listdir(directory):
        if file.endswith(".xml"):
            xml_path = os.path.join(directory, file)
            logger.info(f"Processing: {file}")
            try:
                result = run_migration(xml_path, skip_deploy=skip_deploy)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed: {file}: {e}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Direct execution
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    xml_path = os.path.join(project_root, "input_data", "WF_POL_WKD_FILE_COSTCENTER_LOW.xml")
    run_migration(xml_path, skip_deploy=True)