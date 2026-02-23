#!/usr/bin/env bash
# ------------------------------------
# run_apply_ml_views_bq.sh
#
# Erstellt/aktualisiert ML CORE/MARTS Views in BigQuery via bq CLI.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_apply_ml_views_bq.sh --project <project>
# ------------------------------------

set -euo pipefail
python3 -m bigquery.core_transform.apply_ml_views_with_bq_cli "$@"
