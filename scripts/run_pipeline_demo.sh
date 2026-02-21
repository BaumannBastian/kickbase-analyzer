#!/usr/bin/env bash
# ------------------------------------
# run_pipeline_demo.sh
#
# End-to-end Demo-Run: local ingestion, Databricks-Job-Skeleton,
# danach lokale MARTS-Erzeugung fuer direkt bewertbare Outputs.
#
# Usage
# ------------------------------------
# - ./scripts/run_pipeline_demo.sh
# - ./scripts/run_pipeline_demo.sh --timestamp 2026-02-21T131500Z
# ------------------------------------

set -euo pipefail

python3 -m local_ingestion.runners.run_ingestion --mode demo "$@"
python3 -m databricks.jobs.bronze_ingest.run_bronze_ingest "$@"
python3 -m databricks.jobs.silver_sync.run_silver_sync "$@"
python3 -m databricks.jobs.gold_features.run_gold_features "$@"
python3 -m bigquery.marts.build_marts_local "$@"
