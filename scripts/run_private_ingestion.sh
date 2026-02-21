#!/usr/bin/env bash
# ------------------------------------
# run_private_ingestion.sh
#
# Startet die lokale Ingestion im Private-Mode mit `.env`.
#
# Usage
# ------------------------------------
# - ./scripts/run_private_ingestion.sh
# - ./scripts/run_private_ingestion.sh --env-file .env --out-dir data/bronze
# ------------------------------------

set -euo pipefail
python3 -m local_ingestion.runners.run_ingestion --mode private "$@"
