#!/usr/bin/env bash
# ------------------------------------
# run_build_marts_local.sh
#
# Erzeugt lokale MARTS-Outputs aus den aktuellen Gold-Snapshots.
#
# Usage
# ------------------------------------
# - ./scripts/run_build_marts_local.sh
# - ./scripts/run_build_marts_local.sh --timestamp 2026-02-21T131500Z
# ------------------------------------

set -euo pipefail

python3 -m bigquery.marts.build_marts_local "$@"
