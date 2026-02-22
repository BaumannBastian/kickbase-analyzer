#!/usr/bin/env bash
# ------------------------------------
# run_flyway_migrate.sh
#
# Fuehrt Flyway-Migrationen fuer die lokale History-DB aus.
#
# Usage
# ------------------------------------
# - ./scripts/run_flyway_migrate.sh
# ------------------------------------

set -euo pipefail

if command -v flyway >/dev/null 2>&1; then
  flyway -configFiles=flyway.conf migrate
  exit 0
fi

echo "flyway binary not found. Falling back to dockerized flyway..."
docker run --rm \
  --network kickbaseanalyzer_default \
  -e PGHOST="${PGHOST:-postgres}" \
  -e PGPORT="${PGPORT:-5432}" \
  -e PGDATABASE="${PGDATABASE:-kickbase_history}" \
  -e PGUSER="${PGUSER:-kickbase}" \
  -e PGPASSWORD="${PGPASSWORD:-kickbase}" \
  -v "$PWD/sql:/flyway/sql" \
  -v "$PWD/flyway.conf:/flyway/conf/flyway.conf" \
  redgate/flyway:10 \
  -configFiles=/flyway/conf/flyway.conf migrate
