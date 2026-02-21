#!/usr/bin/env bash
# ------------------------------------
# run_prepare_raw_exports.sh
#
# Exportiert aktuelle Gold-Snapshots als BigQuery RAW JSONL-Dateien.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_prepare_raw_exports.sh
# - ./scripts/bigquery/run_prepare_raw_exports.sh --timestamp 2026-02-21T133000Z
# ------------------------------------

set -euo pipefail
python3 -m bigquery.raw_load.prepare_raw_exports "$@"
