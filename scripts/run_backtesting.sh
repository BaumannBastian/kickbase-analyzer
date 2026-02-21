#!/usr/bin/env bash
# ------------------------------------
# run_backtesting.sh
#
# Wrapper fuer den lokalen Backtesting-Runner.
# Liest Gold/Silver Snapshots und schreibt Backtesting-CSV Outputs.
#
# Usage
# ------------------------------------
# - ./scripts/run_backtesting.sh
# ------------------------------------

set -euo pipefail
python3 -m scripts.ml.run_backtesting "$@"
