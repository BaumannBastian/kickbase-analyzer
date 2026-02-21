#!/usr/bin/env bash
# ------------------------------------
# run_kickbase_auth_check.sh
#
# Wrapper fuer den Kickbase Auth-Check.
#
# Usage
# ------------------------------------
# - ./scripts/run_kickbase_auth_check.sh --env-file .env
# - ./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots
# ------------------------------------

set -euo pipefail
python3 -m scripts.auth.check_kickbase_auth "$@"
