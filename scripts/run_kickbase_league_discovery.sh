#!/usr/bin/env bash
# ------------------------------------
# run_kickbase_league_discovery.sh
#
# Wrapper fuer Kickbase League Discovery.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; leitet JSON auf stdout weiter.
#
# Usage
# ------------------------------------
# - ./scripts/run_kickbase_league_discovery.sh --env-file .env
# ------------------------------------

set -euo pipefail
python3 -m scripts.auth.discover_kickbase_leagues "$@"
