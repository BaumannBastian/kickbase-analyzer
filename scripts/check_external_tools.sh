#!/usr/bin/env bash
# ------------------------------------
# check_external_tools.sh
#
# Prueft, ob externe CLIs fuer Databricks und BigQuery
# in der aktuellen Shell verfuegbar sind.
# Meldet optional gefundene lokale Power-BI-Desktop-Installationen.
#
# Usage
# ------------------------------------
# - ./scripts/check_external_tools.sh
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"

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

if [ -f "/mnt/c/Users/basti/.databrickscfg" ]; then
  echo "[INFO] Found Windows Databricks config: /mnt/c/Users/basti/.databrickscfg"
fi

if [ -x "/mnt/c/Users/basti/AppData/Local/DatabricksCLI/databricks.exe" ]; then
  echo "[INFO] Found Windows Databricks CLI: /mnt/c/Users/basti/AppData/Local/DatabricksCLI/databricks.exe"
fi

if [ -x "/mnt/c/Program Files/Microsoft Power BI Desktop/bin/PBIDesktop.exe" ]; then
  echo "[INFO] Found Power BI Desktop: /mnt/c/Program Files/Microsoft Power BI Desktop/bin/PBIDesktop.exe"
fi

if [ -x "/mnt/c/Users/basti/AppData/Local/Microsoft/WindowsApps/PBIDesktop.exe" ]; then
  echo "[INFO] Found Power BI Desktop (Store): /mnt/c/Users/basti/AppData/Local/Microsoft/WindowsApps/PBIDesktop.exe"
fi
