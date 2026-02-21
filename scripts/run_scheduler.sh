#!/usr/bin/env bash
# ------------------------------------
# run_scheduler.sh
#
# Startet den lokalen Ingestion-Scheduler.
#
# Usage
# ------------------------------------
# - ./scripts/run_scheduler.sh --mode private --interval-seconds 1800
# - ./scripts/run_scheduler.sh --mode demo --max-runs 1
# ------------------------------------

set -euo pipefail
python3 -m local_ingestion.runners.run_scheduler "$@"
