#!/usr/bin/env bash
# ------------------------------------
# run_load_raw_bq.sh
#
# Laedt RAW JSONL-Dateien nach BigQuery via bq CLI.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_load_raw_bq.sh --project <project>
# - ./scripts/bigquery/run_load_raw_bq.sh --project <project> --write-disposition truncate
# ------------------------------------

set -euo pipefail
python3 -m bigquery.raw_load.load_raw_with_bq_cli "$@"
