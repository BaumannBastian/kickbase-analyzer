#!/usr/bin/env bash
# ------------------------------------
# start_db.sh
#
# Startet die lokale PostgreSQL-Instanz fuer History-ETL.
#
# Usage
# ------------------------------------
# - ./scripts/start_db.sh
# - ./scripts/start_db.sh --with-ui
# ------------------------------------

set -euo pipefail

if [[ "${1:-}" == "--with-ui" ]]; then
  docker compose --profile ui up -d
else
  docker compose up -d postgres
fi

docker compose ps
