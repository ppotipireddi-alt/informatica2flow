"""
main.py
-------
CLI entry point for the Informatica → NiFi Agentic Migration System.

Usage:
  # Start the MCP server (agent tool interface)
  python main.py --mode mcp-server

  # Migrate a single Informatica XML file
  python main.py --mode migrate --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml

  # Migrate without deploying to NiFi (export JSON only)
  python main.py --mode migrate --xml input_data/WF.xml --no-deploy

  # Batch migrate all XML files in a directory
  python main.py --mode batch --dir input_data/

  # Translate a single expression (quick test)
  python main.py --mode translate --expr "IIF(AMT > 0, AMT, 0)"

  # Detect Spark in an XML file
  python main.py --mode detect-spark --xml input_data/WF.xml
"""

import argparse
import sys
import os

# Ensure src/ directory is on the path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))


def main():
    parser = argparse.ArgumentParser(
        description="Informatica → NiFi Agentic Migration System with MCP Server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["mcp-server", "migrate", "batch", "translate", "detect-spark"],
        required=True,
        help="Operation mode",
    )
    parser.add_argument("--xml",  type=str, help="Path to Informatica XML file")
    parser.add_argument("--dir",  type=str, help="Directory with XML files (batch mode)")
    parser.add_argument("--expr", type=str, help="Informatica expression to translate")
    parser.add_argument("--no-deploy", action="store_true", help="Skip NiFi deployment (export JSON only)")
    parser.add_argument("--output-dir", type=str, default="output_data", help="Output directory for flow JSON")

    args = parser.parse_args()

    # ── MCP Server ────────────────────────────────────────────────────────────
    if args.mode == "mcp-server":
        print("🚀 Starting Informatica → NiFi MCP Server...")
        print("   Tools available: parse_informatica, detect_spark, translate_expression,")
        print("                    map_datatype, convert_transformation, generate_nifi_flow,")
        print("                    validate_flow, deploy_to_nifi")
        from mcp_server import run_server
        run_server()

    # ── Single Migration ──────────────────────────────────────────────────────
    elif args.mode == "migrate":
        if not args.xml:
            parser.error("--xml is required for migrate mode")
        xml_path = os.path.abspath(args.xml)
        if not os.path.exists(xml_path):
            print(f"❌ XML file not found: {xml_path}")
            sys.exit(1)

        print(f"🔄 Migrating: {xml_path}")
        from langgraph_pipeline import run_migration
        result = run_migration(xml_path, skip_deploy=args.no_deploy)

        export_path = result.get("export_path")
        if export_path:
            print(f"✅ NiFi flow exported to: {export_path}")
        spark = result.get("spark_report", {})
        if spark.get("has_spark"):
            print(f"⚠️  Spark components detected — check review_queue/ for manual migration items")

    # ── Batch Migration ───────────────────────────────────────────────────────
    elif args.mode == "batch":
        if not args.dir:
            parser.error("--dir is required for batch mode")
        input_dir = os.path.abspath(args.dir)
        if not os.path.isdir(input_dir):
            print(f"❌ Directory not found: {input_dir}")
            sys.exit(1)

        xml_files = [f for f in os.listdir(input_dir) if f.endswith(".xml")]
        if not xml_files:
            print(f"❌ No XML files found in: {input_dir}")
            sys.exit(1)

        print(f"🔄 Batch migrating {len(xml_files)} file(s) from: {input_dir}")
        from langgraph_pipeline import run_batch
        results = run_batch(input_dir, skip_deploy=args.no_deploy)

        success = sum(1 for r in results if not r.get("review_required"))
        review = sum(1 for r in results if r.get("review_required"))
        print(f"\n📊 Batch Summary: {len(results)} processed | {success} migrated | {review} need review")

    # ── Expression Translation ────────────────────────────────────────────────
    elif args.mode == "translate":
        if not args.expr:
            parser.error("--expr is required for translate mode")
        from expression_translator import ExpressionTranslator
        translator = ExpressionTranslator(rule_engine=None, use_llm=True)
        translated, confidence, method = translator.translate(args.expr, target="sql")
        print(f"\nOriginal:   {args.expr!r}")
        print(f"Translated: {translated!r}")
        print(f"Method:     {method} | Confidence: {confidence:.2%}")

    # ── Spark Detection ───────────────────────────────────────────────────────
    elif args.mode == "detect-spark":
        if not args.xml:
            parser.error("--xml is required for detect-spark mode")
        xml_path = os.path.abspath(args.xml)
        if not os.path.exists(xml_path):
            print(f"❌ XML file not found: {xml_path}")
            sys.exit(1)

        from informatica_parser import InformaticaXMLParser
        from spark_detector import SparkDetector
        parser_inst = InformaticaXMLParser(xml_path)
        parsed = parser_inst.parse()
        detector = SparkDetector()
        report = detector.detect(parsed)
        print(report.summary())


if __name__ == "__main__":
    main()
