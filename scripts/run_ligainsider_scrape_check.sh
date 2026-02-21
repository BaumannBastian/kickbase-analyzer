#!/usr/bin/env bash
# ------------------------------------
# run_ligainsider_scrape_check.sh
#
# Wrapper fuer den LigaInsider Scrape-Check.
#
# Usage
# ------------------------------------
# - ./scripts/run_ligainsider_scrape_check.sh --env-file .env
# ------------------------------------

set -euo pipefail
python3 -m scripts.auth.check_ligainsider_scrape "$@"
