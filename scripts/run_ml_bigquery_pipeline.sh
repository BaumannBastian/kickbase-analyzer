#!/usr/bin/env bash
# ------------------------------------
# run_ml_bigquery_pipeline.sh
#
# Startet die lokale ML->BigQuery RAW Pipeline:
# 1) ML Training/CV + Champion
# 2) ML RAW Export
# 3) Optional BigQuery RAW Upload
#
# Usage
# ------------------------------------
# - ./scripts/run_ml_bigquery_pipeline.sh --env-file .env --project kickbase-analyzer
# - ./scripts/run_ml_bigquery_pipeline.sh --env-file .env --skip-torch --skip-bq-load
# ------------------------------------

set -euo pipefail
python3 -m scripts.ml.run_ml_bigquery_pipeline "$@"
