#!/usr/bin/env bash
# ------------------------------------
# run_powerbi_api.sh
#
# Wrapper fuer das Power BI REST API Skript.
#
# Usage
# ------------------------------------
# - ./scripts/powerbi/run_powerbi_api.sh list-workspaces
# - ./scripts/powerbi/run_powerbi_api.sh list-datasets --workspace-id <workspace_id>
# ------------------------------------

set -euo pipefail
python3 -m scripts.powerbi.powerbi_api "$@"
