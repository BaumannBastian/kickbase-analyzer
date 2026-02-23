#!/usr/bin/env bash
# ------------------------------------
# run_prepare_history_exports.sh
#
# Exportiert selektive Postgres-History und aktuelle
# Live-Snapshots aus Bronze als BigQuery-RAW JSONL.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_prepare_history_exports.sh --env-file .env
# - ./scripts/bigquery/run_prepare_history_exports.sh --skip-live-snapshots
# ------------------------------------

set -euo pipefail
python3 -m bigquery.raw_load.prepare_history_exports "$@"
