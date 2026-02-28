# Informatica → NiFi Agentic Migration System

An AI-powered migration tool that automatically converts Informatica PowerCenter workflows (XML exports) into Apache NiFi flows. Uses LangGraph for orchestration, LLM-assisted expression translation, and direct NiFi REST API deployment.

## Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                        LangGraph Pipeline                        │
│                                                                  │
│  XML ──▶ Parse ──▶ Spark ──▶ Convert ──▶ Generate ──▶ Validate  │
│           │       Detect     Exprs        NiFi Flow     & Export │
│           │                    │                          │      │
│     informatica_     expression_        nifi_flow_    nifi_direct│
│     parser.py        translator.py      generator.py deployer.py│
└───────────────────────────────────────────────────────────────────┘
```

### Components

| Module | Description |
|---|---|
| `informatica_parser.py` | Parses Informatica XML into sources, targets, transformations, and connectors |
| `spark_detector.py` | Detects Spark/Scala dependencies that cannot be auto-migrated |
| `expression_translator.py` | Translates Informatica expressions to NiFi-compatible SQL (regex + LLM) |
| `datatype_mapper.py` | Maps Informatica datatypes to NiFi/Avro/SQL equivalents |
| `nifi_flow_generator.py` | Generates NiFi flow JSON with processors, connections, and controller services |
| `nifi_direct_deployer.py` | Deploys flows to NiFi via REST API (no XML templates) |
| `langgraph_pipeline.py` | Orchestrates the end-to-end migration pipeline |
| `mcp_server.py` | MCP server exposing migration tools for AI agent integration |
| `confidence_engine.py` | Scores translation confidence for human review routing |
| `validation_engine.py` | Validates generated NiFi flows for completeness |
| `rule_memory_engine.py` | Stores and retrieves learned translation rules |

## Prerequisites

- **Python** ≥ 3.11
- **Docker** (for NiFi and PostgreSQL)
- **uv** package manager
- **OpenAI API key** (for LLM-assisted expression translation)

## Setup

### 1. Install dependencies

```bash
uv sync
```

### 2. Configure environment

Copy and edit the `.env` file:

```bash
cp .env.example .env
```

Key variables:
```env
# NiFi
NIFI_URL=https://localhost:8443
NIFI_USER=admin
NIFI_PASS=adminpassword123

# Database (rule memory)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=password

# LLM
OPENAI_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
```

### 3. Start NiFi (Docker)

```bash
cd nifi-docker
docker-compose up -d
```

NiFi UI will be available at: `https://localhost:8443/nifi`
- Username: `admin`
- Password: `adminpassword123`

### 4. Start PostgreSQL (Docker)

```bash
cd pg_docker
docker-compose up -d
```

## Usage

### Migrate a single workflow

```bash
# Generate NiFi flow JSON (no deployment)
uv run python main.py --mode migrate --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml --no-deploy

# Generate and deploy to NiFi
uv run python main.py --mode migrate --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml
```

### Apply human review corrections

After editing the review JSON in `review_queue/`:

```bash
# Regenerate flow only (no deploy)
uv run python main.py --mode apply-review \
  --review review_queue/M_POL_WKD_FILE_COSTCENTER_review.json \
  --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml --no-deploy

# Regenerate + deploy to NiFi
uv run python main.py --mode apply-review \
  --review review_queue/M_POL_WKD_FILE_COSTCENTER_review.json \
  --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml
```

### Deploy to NiFi (standalone)

```bash
uv run python src/nifi_direct_deployer.py
```

### Batch migrate

```bash
uv run python main.py --mode batch --dir input_data/
```

### Translate a single expression

```bash
uv run python main.py --mode translate --expr "IIF(AMT > 0, AMT, 0)"
```

### Detect Spark dependencies

```bash
uv run python main.py --mode detect-spark --xml input_data/WF.xml
```

### Start MCP Server

```bash
uv run python main.py --mode mcp-server
```

Exposes tools for AI agent integration: `parse_informatica`, `detect_spark`, `translate_expression`, `convert_transformation`, `generate_nifi_flow`, `validate_flow`, `deploy_to_nifi`.

## How It Works

### Migration Pipeline

1. **Parse** — Extracts sources, targets, transformations, connectors, and expressions from Informatica XML
2. **Spark Detection** — Identifies Spark/Scala components that need manual migration; routes them to `review_queue/`
3. **Expression Translation** — Converts Informatica expressions to NiFi-compatible SQL using regex rules first, falling back to LLM for complex expressions
4. **Flow Generation** — Maps Informatica components to NiFi processors:

   | Informatica | NiFi Processor |
   |---|---|
   | Source (table) | `GenerateTableFetch` |
   | Source Qualifier | `GenerateTableFetch` |
   | Expression | `QueryRecord` (SQL) |
   | Filter | `RouteOnAttribute` |
   | Target (file) | `PutFile` |
   | Target (table) | `PutDatabaseRecord` |

5. **Connection Healing** — Automatically resolves implicit Informatica source→SQ relationships and creates missing target processors
6. **Validation** — Checks flow completeness, connection integrity, and relationship correctness
7. **Deployment** — Creates process group with processors, connections, and controller services via NiFi REST API

### Output

- `output_data/<mapping>_nifi_flow.json` — NiFi flow definition
- `review_queue/<mapping>_review.json` — Expressions requiring human review (low confidence or Spark)

---

## Human-in-the-Loop Learning Cycle

The system improves over time. Low-confidence translations are written to `review_queue/` for human correction. Once corrected, those fixes are stored as deterministic rules in the rule memory database and reused automatically on future migrations — no LLM call needed.

### Cycle overview

```
┌─────────────────────────────────────────────────────────────────┐
│  1. migrate --no-deploy                                         │
│     └─▶ confidence < 0.75 ──▶ review_queue/*_review.json       │
│                                                                 │
│  2. Human opens review JSON, fixes "translated" fields          │
│                                                                 │
│  3. apply-review                                                │
│     └─▶ injects corrections into parsed ports                  │
│     └─▶ persists rules to PostgreSQL (deterministic_rules)      │
│     └─▶ regenerates + optionally deploys NiFi flow             │
│                                                                 │
│  4. Next migrate run                                            │
│     └─▶ rule memory hit (confidence 0.9) ──▶ no LLM call       │
└─────────────────────────────────────────────────────────────────┘
```

### Step 1 — Run migration and identify items for review

```bash
uv run python main.py --mode migrate \
  --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml --no-deploy
```

Outputs `review_queue/M_POL_WKD_FILE_COSTCENTER_review.json` when overall confidence < 75%.

### Step 2 — Open the review JSON and edit corrections

The review file contains one entry per translated expression. Edit the `"translated"` field for any incorrect translation:

```json
{
  "transformation": "EXP_FILES",
  "port": "BUS_AREA_HPx",
  "original": "BUS_AREA || '-' || 'HPE'",
  "translated": "CONCAT(BUS_AREA, '-HPE')",
  "confidence": 0.88,
  "method": "regex"
}
```

Focus on entries with **confidence ≤ 0.30** — these are flagged as unreliable.

### Step 3 — Apply the corrections

```bash
uv run python main.py --mode apply-review \
  --review review_queue/M_POL_WKD_FILE_COSTCENTER_review.json \
  --xml input_data/WF_POL_WKD_FILE_COSTCENTER_LOW.xml --no-deploy
```

Output:
```
✏️  Applied 29 human correction(s)
📚 Persisted 12 rule(s) to rule memory — won't need LLM next time
✅ Flow regenerated: output_data/M_POL_WKD_FILE_COSTCENTER_nifi_flow.json
```

### What gets persisted

For each corrected expression (human-changed value **or** confidence < 0.75), two rows are written:

| Table | Content |
|---|---|
| `human_feedback` | Full audit trail — original, LLM output, corrected value, reason |
| `deterministic_rules` | `(inf_pattern → nifi_replacement)` at confidence **0.9**, `approved=TRUE` |

### Step 4 — Future migrations use learned rules

Next time the same Informatica expression appears in any mapping:
1. Expression translator checks `deterministic_rules` first
2. Finds the human-approved rule → returns at **0.9 confidence** immediately
3. LLM is never called for that expression again

Over time the rule memory grows and LLM usage drops.

## Project Structure

```
├── main.py                      # CLI entry point
├── src/
│   ├── informatica_parser.py    # XML parser
│   ├── spark_detector.py        # Spark/Scala detector
│   ├── expression_translator.py # Expression translation (regex + LLM)
│   ├── datatype_mapper.py       # Datatype mapping
│   ├── nifi_flow_generator.py   # NiFi flow JSON generator
│   ├── nifi_direct_deployer.py  # REST API deployer
│   ├── langgraph_pipeline.py    # LangGraph orchestration
│   ├── apply_review.py          # Human review re-apply + rule learning
│   ├── mcp_server.py            # MCP server
│   ├── confidence_engine.py     # Translation confidence scoring
│   ├── validation_engine.py     # Flow validation
│   └── rule_memory_engine.py    # Learned rules storage (PostgreSQL)
├── input_data/                  # Informatica XML files
├── output_data/                 # Generated NiFi flows
├── review_queue/                # Human review packets (low-confidence translations)
├── nifi-docker/                 # NiFi Docker setup
└── pg_docker/                   # PostgreSQL Docker setup
```
