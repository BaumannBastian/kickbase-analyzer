#!/usr/bin/env bash
# ------------------------------------
# run_demo_ingestion.sh
#
# Startet die lokale Ingestion im Demo-Mode.
#
# Usage
# ------------------------------------
# - ./scripts/run_demo_ingestion.sh
# - ./scripts/run_demo_ingestion.sh --timestamp 2026-02-21T120000Z
# ------------------------------------

set -euo pipefail
python3 -m local_ingestion.runners.run_ingestion --mode demo "$@"
