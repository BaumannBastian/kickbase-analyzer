#!/usr/bin/env bash
# ------------------------------------
# run_load_ml_raw_bq.sh
#
# Laedt ML-RAW JSONL-Dateien nach BigQuery via bq CLI.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_load_ml_raw_bq.sh --project <project>
# - ./scripts/bigquery/run_load_ml_raw_bq.sh --project <project> --write-disposition truncate
# ------------------------------------

set -euo pipefail
"$(dirname "$0")/bootstrap_bq_auth.sh" "$@"
python3 -m bigquery.raw_load.load_ml_with_bq_cli "$@"
