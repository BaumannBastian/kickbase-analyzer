#!/usr/bin/env bash
# ------------------------------------
# check_external_tools.sh
#
# Prueft, ob externe CLIs fuer Databricks, BigQuery und Power BI
# in der aktuellen Shell verfuegbar sind.
#
# Usage
# ------------------------------------
# - ./scripts/check_external_tools.sh
# ------------------------------------

set -euo pipefail

check_tool() {
  local tool="$1"
  if command -v "$tool" >/dev/null 2>&1; then
    printf "[OK] %s -> %s\n" "$tool" "$(command -v "$tool")"
  else
    printf "[MISSING] %s\n" "$tool"
  fi
}

check_tool databricks
check_tool bq
check_tool gcloud
check_tool pwsh
