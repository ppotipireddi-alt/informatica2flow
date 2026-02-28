"""
apply_review.py
---------------
Re-generate a NiFi flow from a human-edited review JSON file.

After the migration pipeline routes a mapping to review_queue/ for human
correction, the human edits the `translated` field in the review JSON.
This script reads those corrections, injects them back into the parsed
mapping, and re-runs conversion + export (skipping expensive LLM steps).

Usage:
  uv run python src/apply_review.py \\
    --review review_queue/M_POL_WKD_FILE_COSTCENTER_review.json \\
    --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml

  # Also deploy after regenerating:
  uv run python src/apply_review.py \\
    --review review_queue/M_POL_WKD_FILE_COSTCENTER_review.json \\
    --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml \\
    --deploy
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ApplyReview")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def apply_review(review_json_path: str, xml_path: str, deploy: bool = False) -> dict:
    """
    Apply human corrections from a review JSON and regenerate the NiFi flow.

    Steps:
      1. Load the human-edited review JSON
      2. Connect to rule memory engine
      3. Re-parse the original Informatica XML
      4. Inject corrected expressions into parsed ports AND persist to rule memory
      5. Re-run NiFi intermediate conversion + flow export
      6. Optionally deploy to NiFi via REST API
    """
    # ── 1. Load review JSON ────────────────────────────────────────────────────
    with open(review_json_path) as f:
        review = json.load(f)

    mapping_name = review.get("mapping_name", "unknown")
    translations = review.get("expression_translations", [])
    logger.info(f"Loaded review for: {mapping_name} ({len(translations)} expressions)")

    # Build correction lookup: (transformation_name, port_name) → item
    items_by_key = {
        (item["transformation"], item["port"]): item
        for item in translations
    }
    logger.info(f"Corrections ready: {len(items_by_key)}")

    # ── 2. Connect to rule memory (early, so we can persist corrections) ───────
    from rule_memory_engine import RuleMemoryEngine
    from config import DB_CONFIG
    try:
        rule_engine = RuleMemoryEngine(DB_CONFIG)
        logger.info("Rule memory engine connected")
    except Exception as e:
        logger.warning(f"Rule engine unavailable — corrections will NOT be persisted: {e}")
        rule_engine = None

    # ── 3. Re-parse original XML ───────────────────────────────────────────────
    from informatica_parser import InformaticaXMLParser
    parser = InformaticaXMLParser(xml_path)
    parsed = parser.parse()
    logger.info(f"Re-parsed XML: {parsed.get('mapping_name')}")

    # ── 4. Inject corrections + persist changed ones to rule memory ────────────
    applied = 0
    learned = 0
    for transformation in parsed.get("transformations", []):
        t_name = transformation.get("name", "")
        for port in transformation.get("ports", []):
            p_name = port.get("name", "")
            key = (t_name, p_name)
            if key not in items_by_key:
                continue

            item = items_by_key[key]
            original_expr = item.get("original", "")
            old_translated = item.get("translated",
                port.get("expression_translated") or port.get("expression", ""))
            # The corrected value is whatever is now in the review JSON's 'translated' field
            corrected = item["translated"]

            port["expression_translated"] = corrected
            logger.info(f"  ✏️  {t_name}.{p_name}")
            logger.info(f"       original:  {original_expr!r}")
            logger.info(f"       corrected: {corrected!r}")
            applied += 1

            # Persist to rule memory only when the human changed the value
            # (skip passthrough-unchanged entries that are already correct)
            was_changed = corrected.strip() != old_translated.strip()
            was_low_confidence = item.get("confidence", 1.0) < 0.75

            if rule_engine and original_expr and (was_changed or was_low_confidence):
                try:
                    rule_engine.store_human_feedback(
                        original=original_expr,
                        llm_output=old_translated,
                        corrected=corrected,
                        reason=f"Human review: {mapping_name} / {t_name}.{p_name}",
                    )
                    learned += 1
                    logger.info(f"  📚 Persisted rule: {original_expr!r} → {corrected!r}")
                except Exception as e:
                    logger.warning(f"  Could not persist rule for {t_name}.{p_name}: {e}")

    print(f"\n✏️  Applied {applied} human correction(s)")
    if rule_engine:
        print(f"📚 Persisted {learned} rule(s) to rule memory — won't need LLM next time")
    else:
        print("⚠️  Rule memory unavailable — corrections not persisted")

    # ── 5. Re-run conversion ───────────────────────────────────────────────────
    from nifi_intermediate_converter import NiFiIntermediateConverter
    converter = NiFiIntermediateConverter(parsed_mapping=parsed, rule_engine=rule_engine)
    intermediate = converter.convert()
    logger.info(f"Conversion done: {len(intermediate.get('processors', []))} processors")

    # ── 5. Re-generate NiFi flow JSON ─────────────────────────────────────────
    from nifi_flow_generator import NiFiFlowGenerator
    generator = NiFiFlowGenerator(output_dir="output_data")
    export_path = generator.export(parsed, intermediate)

    print(f"✅ Flow regenerated: {export_path}")

    result = {"export_path": export_path}

    # ── 6. Optionally deploy ───────────────────────────────────────────────────
    if deploy:
        from nifi_direct_deployer import NiFiDirectDeployer
        from config import NIFI_CONFIG
        deployer = NiFiDirectDeployer(
            NIFI_CONFIG["url"],
            NIFI_CONFIG["username"],
            NIFI_CONFIG["password"],
        )
        pg_id = deployer.deploy_flow_json(export_path)
        print(f"✅ Deployed to NiFi — Process Group ID: {pg_id}")
        result["deployed_group_id"] = pg_id

    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Re-apply human-reviewed translations to a NiFi flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--review", required=True,
        help="Path to the human-edited review JSON (e.g. review_queue/M_POL_WKD_FILE_COSTCENTER_review.json)"
    )
    parser.add_argument(
        "--xml", required=True,
        help="Path to the original Informatica XML file"
    )
    parser.add_argument(
        "--deploy", action="store_true",
        help="Deploy the regenerated flow to NiFi after exporting"
    )
    args = parser.parse_args()

    review_path = os.path.abspath(args.review)
    xml_path    = os.path.abspath(args.xml)

    if not os.path.exists(review_path):
        print(f"❌ Review file not found: {review_path}")
        sys.exit(1)
    if not os.path.exists(xml_path):
        print(f"❌ XML file not found: {xml_path}")
        sys.exit(1)

    apply_review(review_path, xml_path, deploy=args.deploy)


if __name__ == "__main__":
    main()
