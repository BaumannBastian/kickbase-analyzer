#!/usr/bin/env bash
# ------------------------------------
# run_bigquery_pipeline.sh
#
# End-to-end BigQuery-Flow:
# 1) Gold -> RAW JSONL export
# 2) RAW Upload in BigQuery
# 3) CORE/MARTS Views anwenden
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_bigquery_pipeline.sh --project <project>
# - ./scripts/bigquery/run_bigquery_pipeline.sh --project <project> --timestamp 2026-02-21T133000Z
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"

python3 -m bigquery.raw_load.prepare_raw_exports "$@"
python3 -m bigquery.raw_load.load_raw_with_bq_cli "$@"
python3 -m bigquery.core_transform.apply_views_with_bq_cli "$@"
