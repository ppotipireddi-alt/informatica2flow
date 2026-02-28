# ---------------------------------------------------------
# State Definition
# ---------------------------------------------------------

from typing import TypedDict, List, Dict, Any, Optional


class MigrationState(TypedDict, total=False):
    # Input
    xml_path: str

    # Parsing
    parsed_mapping: Dict[str, Any]
    complexity: str

    # Spark detection
    spark_report: Dict[str, Any]
    unsupported_items: List[Any]

    # Expression translation
    expression_translations: List[Dict[str, Any]]

    # Intermediate NiFi model (from NiFiIntermediateConverter)
    nifi_intermediate: Dict[str, Any]

    # Complete NiFi flow JSON (from NiFiFlowGenerator)
    nifi_flow_json: Dict[str, Any]
    export_path: str

    # Validation & Confidence
    validation_report: Dict[str, Any]
    confidence_scores: List[float]
    overall_confidence: float
    review_required: bool

    # Deployment
    deployed_group_id: str
