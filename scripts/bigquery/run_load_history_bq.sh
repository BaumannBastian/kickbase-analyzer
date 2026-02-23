#!/usr/bin/env bash
# ------------------------------------
# run_load_history_bq.sh
#
# Laedt selektive History-RAW JSONL nach BigQuery via bq CLI.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_load_history_bq.sh --project <project>
# - ./scripts/bigquery/run_load_history_bq.sh --project <project> --write-disposition truncate
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"
"$(dirname "$0")/bootstrap_bq_auth.sh" "$@"
python3 -m bigquery.raw_load.load_history_with_bq_cli "$@"
