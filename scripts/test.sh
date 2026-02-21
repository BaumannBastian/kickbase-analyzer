#!/usr/bin/env bash
# ------------------------------------
# test.sh
#
# Fuehrt alle Unit-Tests aus.
#
# Usage
# ------------------------------------
# - ./scripts/test.sh
# ------------------------------------

set -euo pipefail
python3 -m unittest discover -s tests -p 'test_*.py' -v
