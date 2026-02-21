#!/usr/bin/env bash
# ------------------------------------
# run_databricks_jobs_demo.sh
#
# Fuehrt die lokalen Databricks-Job-Skeletons in Reihenfolge aus:
# Bronze ingest -> Silver sync -> Gold features.
#
# Usage
# ------------------------------------
# - ./scripts/run_databricks_jobs_demo.sh
# - ./scripts/run_databricks_jobs_demo.sh --timestamp 2026-02-21T131500Z
# ------------------------------------

set -euo pipefail

python3 -m databricks.jobs.bronze_ingest.run_bronze_ingest "$@"
python3 -m databricks.jobs.silver_sync.run_silver_sync "$@"
python3 -m databricks.jobs.gold_features.run_gold_features "$@"
