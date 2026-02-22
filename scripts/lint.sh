#!/usr/bin/env bash
# ------------------------------------
# lint.sh
#
# Fuehrt einen schnellen Syntax-/Compile-Check aus.
#
# Usage
# ------------------------------------
# - ./scripts/lint.sh
# ------------------------------------

set -euo pipefail
python3 -m compileall -q local_ingestion databricks bigquery scripts src tests
