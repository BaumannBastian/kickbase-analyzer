#!/usr/bin/env bash
# ------------------------------------
# run_prepare_ml_exports.sh
#
# Exportiert den letzten (oder angegebenen) ML-Run als BigQuery-RAW JSONL.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_prepare_ml_exports.sh
# - ./scripts/bigquery/run_prepare_ml_exports.sh --run-ts 2026-02-23T111527Z
# ------------------------------------

set -euo pipefail
python3 -m bigquery.raw_load.prepare_ml_exports "$@"
