#!/usr/bin/env bash
# ------------------------------------
# run_history_etl.sh
#
# Startet den lokalen Postgres-History-ETL mit CLI-Parametern.
#
# Usage
# ------------------------------------
# - ./scripts/run_history_etl.sh --player-name-like orban --max-players 1
# ------------------------------------

set -euo pipefail
python3 -m src.etl_history "$@"
