#!/usr/bin/env bash
# ------------------------------------
# configure_gcloud_auth.sh
#
# Richtet gcloud/bq Auth fuer lokale Nutzung ein.
# Fuehrt Benutzer-Login und optional ADC Login aus
# und setzt das Standardprojekt.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/configure_gcloud_auth.sh --project <gcp_project_id>
# - ./scripts/bigquery/configure_gcloud_auth.sh --project <gcp_project_id> --skip-adc
# ------------------------------------

set -euo pipefail

PROJECT_ID=""
SKIP_ADC="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --project)
      PROJECT_ID="${2:-}"
      shift 2
      ;;
    --skip-adc)
      SKIP_ADC="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [ -z "${PROJECT_ID}" ]; then
  echo "Missing --project <gcp_project_id>" >&2
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud not found in PATH." >&2
  exit 1
fi

echo "Running gcloud auth login"
gcloud auth login

if [ "${SKIP_ADC}" != "true" ]; then
  echo "Running gcloud auth application-default login"
  gcloud auth application-default login
fi

echo "Setting default project: ${PROJECT_ID}"
gcloud config set project "${PROJECT_ID}"

echo "Active account:"
gcloud auth list --filter=status:ACTIVE --format="value(account)"
echo "Active project:"
gcloud config get-value project
