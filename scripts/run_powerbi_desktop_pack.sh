#!/usr/bin/env bash
# ------------------------------------
# run_powerbi_desktop_pack.sh
#
# Wrapper fuer den Export lokaler Power-BI-Desktop-Assets
# (M Queries, DAX, TMDL) aus versionierten Templates.
#
# Outputs
# ------------------------------------
# 1) dashboards/powerbi/local/desktop_pack_<timestamp>/*
#
# Usage
# ------------------------------------
# - ./scripts/run_powerbi_desktop_pack.sh
# - ./scripts/run_powerbi_desktop_pack.sh --project kickbase-analyzer
# ------------------------------------

set -euo pipefail
python3 -m scripts.powerbi_desktop.export_desktop_assets "$@"

