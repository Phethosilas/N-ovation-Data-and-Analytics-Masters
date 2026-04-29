# Architecture Decision Record: Stage 3 Streaming Extension

**Author:** Challenge Participant
**Date:** April 29, 2026
**Status:** Final

---

## Context

Stage 3 introduced a real-time transaction event stream requirement for the mobile product team. The fintech partner provides a micro-batch JSONL stream delivered to `/data/stream/`, with files arriving at 5-minute intervals. The pipeline must process these events and update two new Gold layer tables (`current_balances` and `recent_transactions`) within a 5-minute SLA from file arrival to Gold update.

The state of my pipeline entering Stage 3 consisted of approximately 400 lines of code across four modules: `ingest.py` (Bronze layer, 51 lines), `transform.py` (Silver layer with deduplication and referential integrity validation, 90 lines), `provision.py` (Gold dimensional model with surrogate key generation, 170 lines), and `run_all.py` (orchestration, 50 lines). Between Stage 1 and Stage 2, I added a `dq_handler.py` module (160 lines) for data quality detection and remediation, and externalized DQ rules to `config/dq_rules.yaml`. The architecture was modular with clear layer separation, but all processing was batch-oriented with no streaming constructs.

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

**What made Stage 3 easier:**

The modular separation between ingestion (`ingest.py`), transformation (`transform.py`), and provisioning (`provision.py`) significantly reduced Stage 3 implementation effort. I was able to create a standalone `stream_ingest.py` module (140 lines) that reused the existing `get_spark_session()` utility from `common/spark_setup.py` without modification. The configuration-driven path management in `pipeline_config.yaml` allowed me to add a `paths.stream` entry without touching the batch pipeline code. Specifically, my Silver layer's transaction timestamp derivation logic (combining `transaction_date` and `transaction_time` into a single `transaction_timestamp` column in `transform.py` lines 65-68) was directly reusable in the streaming path - I copied the exact same transformation into `stream_ingest.py` line 35, eliminating potential timestamp inconsistencies between batch and stream Gold outputs.

My decision to use Parquet instead of Delta format (due to the base image JAR issue documented in `DELTA_LAKE_ISSUE.md`) actually simplified the streaming implementation. Reading and writing Parquet with `mode("overwrite")` for the upsert semantics in `current_balances` required no special merge syntax. If I had been using Delta format with proper `MERGE INTO` operations, the streaming upsert logic would have been more complex but also more production-ready.

**What made Stage 3 harder:**

The monolithic `run_all.py` entry point (lines 22-46) combined all three batch layers into a single sequential execution flow with no parameterization. When Stage 3 required a dual-mode pipeline (batch + streaming), I had to add branching conditional logic to detect whether streaming files exist and fork execution accordingly. This made the orchestration harder to reason about. If I had designed `run_all.py` from the start to accept a `--mode` argument (`batch` or `stream` or `both`), the Stage 3 extension would have been a simple addition of a `stream` mode branch rather than retrofitting conditionals into the existing batch flow.

My Silver layer deduplication logic in `transform.py` (lines 83-92) was hardcoded to use the basic `deduplicate()` function for all tables. When Stage 2 introduced the DQ handler with more sophisticated deduplication that tracks duplicate counts for the DQ report, I had to add try-except wrapping and feature detection (`if DQ_AVAILABLE`) to avoid breaking Stage 1 compatibility. This created technical debt that complicated the Stage 3 extension - the streaming path also needed deduplication, but I couldn't reuse the DQ handler cleanly because it was tightly coupled to the batch Silver transformation.

**Code survival rate:**

Approximately 75% of my Stage 1/2 code survived intact into Stage 3. The `ingest.py`, `common/spark_setup.py`, and `common/logger.py` modules required zero changes. The `provision.py` module required minor changes to handle nullable `merchant_subcategory` (3 lines modified). The `transform.py` module required one conditional block for DQ handler integration (10 lines added). The `run_all.py` orchestrator required 20 lines of new conditional logic to detect and invoke streaming mode. The remaining 25% was new code: `stream_ingest.py` (140 lines), `dq_handler.py` enhancements (30 lines), and ADR documentation.

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

**Separate schema definitions into a dedicated module:**

I would have created a `config/schemas.py` file containing all Gold table column lists and types as Python constants. In my current implementation, the Gold schema is embedded inline in `provision.py` lines 53-63 (dim_customers), 75-87 (dim_accounts), and 122-138 (fact_transactions). When Stage 3 required two new tables (`current_balances` and `recent_transactions`), I had to define their schemas in `stream_ingest.py` lines 48-53 and 71-76, creating schema duplication across modules. If schemas had been centralized, adding the two new tables would have been a simple addition to `schemas.py`, and both batch and streaming pipelines could reference the same definitions. This would have reduced the risk of schema drift between batch and stream Gold tables.

**Parameterize the entry point from Day 1:**

I would have designed `run_all.py` to accept a `--mode` CLI argument from the start, with values `batch`, `stream`, or `both`. The current implementation (lines 22-46) hardcodes the sequential batch flow with no branching. When Stage 3 arrived, I had to add conditional logic to detect whether `/data/stream/` exists and whether to invoke streaming, which made the entry point harder to test in isolation. A cleaner Day 1 design would have been:

```python
def main():
    mode = os.environ.get('PIPELINE_MODE', 'batch')
    if mode in ['batch', 'both']:
        run_batch_pipeline(config)
    if mode in ['stream', 'both']:
        run_streaming_pipeline(config)
```

This would have kept the batch and streaming paths fully decoupled from the start, rather than the current retrofitted conditional in `run_all.py`.

**Abstract the deduplication logic:**

My `transform.py` deduplicate() function (lines 37-42) is a generic window-function-based deduplicator, but it doesn't track statistics (like duplicate counts for the DQ report). When Stage 2 introduced DQ reporting requirements, I had to create a parallel `deduplicate_transactions()` in `dq_handler.py` that does the same transformation but also returns counts. These two functions are 80% identical code. If I had made the Stage 1 deduplicate() function return a tuple `(deduplicated_df, removed_count)` from the beginning, the Stage 2 DQ extension would have required zero refactoring, and the streaming path could have reused the same function.

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

**Day 1 architecture with full three-stage visibility:**

If I had known the full specification on Day 1, I would have structured the pipeline with the following design:

**Ingestion abstraction:** I would have created a `pipeline/sources/` module with a base `DataSource` class and concrete implementations `CSVSource`, `JSONLSource`, and `StreamingJSONLSource`. Each source would implement a `.read()` method returning a standardized DataFrame. The Bronze layer would iterate over a config-driven list of sources rather than hardcoding three separate read operations. This would have made the Stage 3 streaming source a simple addition of a fourth source to the config, rather than creating an entirely separate `stream_ingest.py` module.

**State management for current_balances:** The `current_balances` table requires maintaining running balance state across stream batches. My current implementation uses Parquet overwrite for the entire table on each batch, which works but is inefficient (full table rewrite every 5 minutes). If I had known this requirement from Day 1, I would have designed the Gold layer to use Delta format from the start (assuming the base image JAR issue was resolved) and implemented `current_balances` as a true upsert using Delta's `MERGE INTO` syntax. This would allow incremental updates: only accounts with new transactions in the current batch would be touched, rather than rewriting the entire table. The state management pattern would also generalize to other streaming aggregations like `recent_transactions` retention.

**Output path structure:** I currently have separate `output/gold/` and `output/stream_gold/` directories. With full visibility, I would have used a single `output/gold/` directory with both batch and streaming tables coexisting, distinguished by table name prefix (`batch_fact_transactions`, `stream_current_balances`). Alternatively, I would have used a `gold/batch/` and `gold/stream/` subdirectory structure from the start, making it explicit that these are parallel output paths with different update semantics (batch = daily full refresh, stream = continuous upsert).

**Pipeline entry point:** Instead of a monolithic `run_all.py`, I would have created two separate executables: `batch_pipeline.py` and `stream_pipeline.py`, both importing from a shared `pipeline/core/` library containing transformation logic, schema definitions, and DQ handlers. The Docker `CMD` would default to running both in sequence (`batch_pipeline.py && stream_pipeline.py`), but local development and testing could invoke them independently. This separation would have made the Stage 3 ADR interrogation simpler - the architectural boundary between batch and streaming would be file-level, not function-level.

**Incremental processing design:** Knowing that Stage 3 would require maintaining "last 50 transactions per account" in `recent_transactions`, I would have implemented windowed aggregations and row retention logic in the batch pipeline from Day 1, even if unused. Specifically, I would have added a `--limit-per-key` parameter to the Gold provisioning step that applies a `row_number()` window and filters to the top N rows per partition key. This pattern, tested and proven on the batch workload at Stage 1 scale, would transfer directly to the streaming workload without requiring new window logic to be written under time pressure at Stage 3.

---

## Appendix

**Architecture Diagram (Text-based):**

```
Day 1 Architecture (if I had full visibility):

┌─────────────────────────────────────────────────────────────┐
│  PIPELINE ENTRY POINTS                                       │
│  ┌──────────────────┐       ┌─────────────────────┐        │
│  │ batch_pipeline.py│       │ stream_pipeline.py  │        │
│  │ (daily full run) │       │ (continuous polling)│        │
│  └────────┬─────────┘       └──────────┬──────────┘        │
│           │                             │                    │
│           └──────────┬──────────────────┘                    │
│                      │                                       │
└──────────────────────┼───────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────────────┐
        │  SHARED CORE LIBRARY             │
        │  pipeline/core/                  │
        │  ├── schemas.py (all tables)     │
        │  ├── transformations.py          │
        │  ├── dq_rules.py                 │
        │  └── state_manager.py (upsert)   │
        └──────────┬────────────────────────┘
                   │
     ┌─────────────┴──────────────┐
     │                            │
     ▼                            ▼
┌─────────────┐           ┌──────────────────┐
│ BATCH GOLD  │           │ STREAMING GOLD   │
│ /gold/      │           │ /gold/stream/    │
│ ├── dim_*   │           │ ├── balances     │
│ └── fact_*  │           │ └── recent_txns  │
└─────────────┘           └──────────────────┘
```

This design keeps batch and streaming pipelines as separate executables sharing a common transformation library, rather than the current retrofitted conditional approach.
