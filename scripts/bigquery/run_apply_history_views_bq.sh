#!/usr/bin/env bash
# ------------------------------------
# run_apply_history_views_bq.sh
#
# Erstellt/aktualisiert History CORE/MARTS Views in BigQuery.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/run_apply_history_views_bq.sh --project <project>
# ------------------------------------

set -euo pipefail
export PATH="$HOME/bin:$PATH"
"$(dirname "$0")/bootstrap_bq_auth.sh" "$@"
python3 -m bigquery.core_transform.apply_history_views_with_bq_cli "$@"
