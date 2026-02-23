#!/usr/bin/env bash
# ------------------------------------
# run_history_bigquery_pipeline.sh
#
# End-to-end selektiver History -> BigQuery Flow:
# 1) Export aus Postgres/Bronze nach RAW JSONL
# 2) RAW Upload nach BigQuery
# 3) History CORE/MARTS Views anwenden
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_history_bigquery_pipeline.sh --env-file .env --project <project>
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"
"$(dirname "$0")/bootstrap_bq_auth.sh" "$@"

python3 -m scripts.bigquery.run_history_bigquery_pipeline "$@"
