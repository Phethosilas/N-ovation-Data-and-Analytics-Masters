# Nedbank DE Challenge - Submission README

**Repository:** https://github.com/Phethosilas/N-ovation-Data-and-Analytics-Masters
**Stages:** 1, 2, and 3 (all implemented)
**Author:** Phethosila S
**Date:** April 29, 2026

---

## Quick Start - Local Testing

### Prerequisites
- Docker installed and running
- Git bash or WSL for running test scripts
- Sample data in proper directory structure

### Build and Run

```bash
# Build the Docker image
docker build -t nedbank-stage1:test .

# Run with test data
docker run --rm \
  --network=none \
  --memory=2g --cpus=2 \
  --read-only \
  --tmpfs /tmp:rw,size=512m \
  -v /path/to/test-data:/data \
  nedbank-stage1:test

# Run automated tests
bash run_tests.sh --stage 1 --data-dir /path/to/test-data --image nedbank-stage1:test --build
```

### Test Data Directory Structure

```
test-data/
├── input/
│   ├── customers.csv
│   ├── accounts.csv
│   └── transactions.jsonl
├── config/
│   ├── pipeline_config.yaml
│   └── dq_rules.yaml (Stage 2+)
├── stream/ (Stage 3 only)
│   ├── stream_20260320_143000_0001.jsonl
│   └── ...
└── output/ (created by pipeline)
    ├── bronze/
    ├── silver/
    ├── gold/
    └── stream_gold/ (Stage 3)
```

---

## Pipeline Architecture

### Medallion Layers

```
Bronze (Raw Ingestion)
  ├── customers/    (Parquet - raw + ingestion_timestamp)
  ├── accounts/     (Parquet - raw + ingestion_timestamp)
  └── transactions/ (Parquet - raw + ingestion_timestamp)

Silver (Standardized & Validated)
  ├── customers/    (Deduplicated, typed, date-standardized)
  ├── accounts/     (Deduplicated, typed, linked to customers)
  └── transactions/ (Deduplicated, timestamped, DQ-flagged)

Gold (Dimensional Model)
  ├── dim_customers/      (9 fields, includes derived age_band)
  ├── dim_accounts/       (11 fields, includes customer_id for joins)
  ├── fact_transactions/  (15 fields, includes dq_flag)
  └── stream_gold/ (Stage 3)
      ├── current_balances/     (4 fields, upsert semantics)
      └── recent_transactions/  (7 fields, last 50 per account)
```

### Modules

| Module | Purpose | Lines | Stage |
|---|---|---|---|
| `pipeline/ingest.py` | Bronze layer ingestion | 51 | All |
| `pipeline/transform.py` | Silver layer transformation | 110 | All |
| `pipeline/provision.py` | Gold dimensional model | 170 | All |
| `pipeline/dq_handler.py` | Data quality detection & remediation | 160 | 2+ |
| `pipeline/stream_ingest.py` | Streaming pipeline (polling & upsert) | 140 | 3 |
| `pipeline/run_all.py` | Orchestration with stage detection | 90 | All |
| `pipeline/common/spark_setup.py` | Spark session management | 40 | All |
| `pipeline/common/logger.py` | Logging configuration | 22 | All |

---

## Stage-Specific Features

### Stage 1: Clean Data Pipeline
- ✅ Bronze → Silver → Gold flow
- ✅ Deduplication on primary keys
- ✅ Referential integrity validation
- ✅ Age band derivation from DOB
- ✅ Surrogate key generation

### Stage 2: Data Quality Extension
- ✅ 6 DQ issue types detected and handled
- ✅ DQ rules externalized to `config/dq_rules.yaml`
- ✅ DQ report generation (`/data/output/dq_report.json`)
- ✅ Currency normalization (all variants → ZAR)
- ✅ Date format standardization (mixed formats → YYYY-MM-DD)
- ✅ Type mismatch handling (STRING amount → DECIMAL)
- ✅ Orphaned transaction quarantine
- ✅ Support for `merchant_subcategory` field (nullable)

### Stage 3: Streaming Extension
- ✅ Directory polling for micro-batch files
- ✅ `current_balances` upsert (running balance per account)
- ✅ `recent_transactions` retention (last 50 per account)
- ✅ 5-minute SLA tracking via `updated_at` timestamp
- ✅ Batch + streaming pipelines execute in same container
- ✅ Architecture Decision Record (ADR) documented

---

## Known Issues and Workarounds

### Delta Lake Format Issue

**Issue:** The base image (`nedbank-de-challenge/base:1.0`) has Delta Lake Python package installed but is missing the required JAR files in Spark's classpath.

**Error:** `java.lang.ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog`

**Workaround Applied:** Switched all `.format("delta")` to `.format("parquet")` throughout the pipeline.

**Impact:** 
- ✅ Pipeline logic is 100% correct and functional
- ✅ All transformations, joins, and aggregations work as specified
- ⚠️ Output format is Parquet instead of Delta (violates challenge spec)

**Resolution:** Full issue report submitted to challenge organizers (see `DELTA_LAKE_ISSUE.md`)

**When Delta is fixed:** Simply uncomment Delta configs in `pipeline_config.yaml` lines 19-20 and change all `format("parquet")` back to `format("delta")` in three files (ingest.py, transform.py, provision.py, stream_ingest.py). No other code changes required.

---

## Validation Queries

All three validation queries pass (tested with DuckDB):

### Query 1: Transaction Volume by Type
- Expected: 4 rows (CREDIT, DEBIT, FEE, REVERSAL)
- ✅ Returns correct transaction type distribution

### Query 2: Zero Orphaned Accounts
- Expected: 0 unlinked accounts
- ✅ All dim_accounts.customer_id link to dim_customers.customer_id

### Query 3: Province Distribution
- Expected: 9 rows (SA provinces)
- ✅ Returns province-to-account mapping via customer join

---

## Configuration Files

### `config/pipeline_config.yaml`
- Input/output paths
- Spark configuration (memory, parallelism)
- Age band bucket definitions
- Stream directory path (Stage 3)

### `config/dq_rules.yaml` (Stage 2+)
- DQ issue type definitions
- Detection methods
- Handling actions (QUARANTINED, NORMALISED, etc.)
- Currency variant mappings

---

## Resource Constraints

Tested under challenge constraints:
- ✅ 2 GB RAM limit
- ✅ 2 vCPU limit
- ✅ Network disabled (`--network=none`)
- ✅ Read-only filesystem (except /data/output and /tmp)
- ✅ 30-minute execution time limit

**Performance:** Stage 1 completes in <5 minutes on sample data

---

## Submission Tags

```bash
# Stage 1
git tag -a stage1-submission -m "Stage 1: Clean data pipeline"
git push origin stage1-submission

# Stage 2  
git tag -a stage2-submission -m "Stage 2: DQ handling + 3x volume"
git push origin stage2-submission

# Stage 3
git tag -a stage3-submission -m "Stage 3: Streaming extension"
git push origin stage3-submission
```

---

## Contact

For questions about this submission:
- Repository: https://github.com/Phethosilas/N-ovation-Data-and-Analytics-Masters
- Issue with base image: See `DELTA_LAKE_ISSUE.md`

---

**Note:** This pipeline is ready for all three stages with the Parquet workaround. Once the base image Delta Lake issue is resolved, switching back to Delta format requires only configuration changes, not code rewrites.
