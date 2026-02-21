#!/usr/bin/env bash
# ------------------------------------
# check_bq_setup.sh
#
# Prueft, ob gcloud/bq verfuegbar sind und zeigt die aktive
# Auth-/Projektkonfiguration an.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/check_bq_setup.sh
# ------------------------------------

set -euo pipefail

if ! command -v gcloud >/dev/null 2>&1; then
  echo "[MISSING] gcloud"
  exit 1
fi

if ! command -v bq >/dev/null 2>&1; then
  echo "[MISSING] bq"
  exit 1
fi

echo "[OK] gcloud -> $(command -v gcloud)"
echo "[OK] bq -> $(command -v bq)"
echo "gcloud version:"
gcloud version | sed -n '1,2p'
echo "bq version:"
bq version
echo "active account:"
gcloud auth list --filter=status:ACTIVE --format="value(account)"
echo "active project:"
gcloud config get-value project
