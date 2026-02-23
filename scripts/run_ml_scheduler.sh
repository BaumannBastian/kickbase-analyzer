#!/usr/bin/env bash
# ------------------------------------
# run_ml_scheduler.sh
#
# Startet den lokalen Scheduler fuer die ML->BigQuery Pipeline.
#
# Usage
# ------------------------------------
# - ./scripts/run_ml_scheduler.sh --interval-seconds 21600 -- --env-file .env --project kickbase-analyzer
# - ./scripts/run_ml_scheduler.sh --max-runs 1 -- --env-file .env --skip-bq-load
# ------------------------------------

set -euo pipefail
python3 -m scripts.ml.run_ml_pipeline_scheduler "$@"
