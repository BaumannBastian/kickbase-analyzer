#!/usr/bin/env bash
# ------------------------------------
# run_apply_views_bq.sh
#
# Erstellt/aktualisiert CORE- und MARTS-Views in BigQuery via bq CLI.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_apply_views_bq.sh --project <project>
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"
python3 -m bigquery.core_transform.apply_views_with_bq_cli "$@"
