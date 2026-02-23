#!/usr/bin/env bash
# ------------------------------------
# run_ml_bigquery_pipeline.sh
#
# End-to-end ML -> BigQuery RAW Pipeline:
# 1) ML Training/CV (inkl. Champion-Auswahl)
# 2) Export ML-Run nach data/warehouse/raw_ml
# 3) Upload der ml_* Tabellen nach BigQuery RAW
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_ml_bigquery_pipeline.sh --env-file .env --project kickbase-analyzer
# - ./scripts/bigquery/run_ml_bigquery_pipeline.sh --env-file .env --skip-bq-load
# ------------------------------------

set -euo pipefail
python3 -m scripts.ml.run_ml_bigquery_pipeline "$@"
