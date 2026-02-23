#!/usr/bin/env bash
# ------------------------------------
# bootstrap_bq_auth.sh
#
# Richtet BigQuery/gcloud Auth fuer Bash/WSL ein, damit
# BQ-Commands ohne PowerShell laufen.
#
# Prioritaet
# ------------------------------------
# 1) Service Account Key (empfohlen fuer vollautomatische Runs)
# 2) Bestehendes CloudSDK-Profil (z.B. Windows gcloud config in WSL)
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/bootstrap_bq_auth.sh --env-file .env --project kickbase-analyzer
# - ./scripts/bigquery/bootstrap_bq_auth.sh --env-file .env
# ------------------------------------

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

ENV_FILE=".env"
PROJECT_ID="${BQ_PROJECT_ID:-}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --env-file)
      ENV_FILE="${2:-.env}"
      shift 2
      ;;
    --project)
      PROJECT_ID="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  set -a
  . "${ENV_FILE}"
  set +a
fi

export PATH="${HOME}/bin:${PATH}"

if ! command -v gcloud >/dev/null 2>&1; then
  echo "[bootstrap_bq_auth] gcloud not found in PATH."
  exit 1
fi

if ! command -v bq >/dev/null 2>&1; then
  echo "[bootstrap_bq_auth] bq not found in PATH."
  exit 1
fi

if [ -n "${BQ_CLOUDSDK_CONFIG:-}" ]; then
  CLOUDSDK_PATH="${BQ_CLOUDSDK_CONFIG}"
  if [[ "${CLOUDSDK_PATH}" == [A-Za-z]:\\* ]]; then
    if command -v wslpath >/dev/null 2>&1; then
      CLOUDSDK_PATH="$(wslpath "${CLOUDSDK_PATH}")"
    fi
  fi
  if [ -d "${CLOUDSDK_PATH}" ]; then
    export CLOUDSDK_CONFIG="${CLOUDSDK_PATH}"
  fi
fi

# Fallback auf repo-lokales Cloud SDK config dir, damit WSL-Runs
# ohne Schreibrechte in ~/.config trotzdem funktionieren.
if [ -z "${CLOUDSDK_CONFIG:-}" ]; then
  export CLOUDSDK_CONFIG="${REPO_ROOT}/.cache/gcloud"
  mkdir -p "${CLOUDSDK_CONFIG}"
fi

SERVICE_KEY="${BQ_SERVICE_ACCOUNT_KEY_FILE:-${GOOGLE_APPLICATION_CREDENTIALS:-}}"
if [ -n "${SERVICE_KEY}" ]; then
  if [ ! -f "${SERVICE_KEY}" ]; then
    if [ -f "${REPO_ROOT}/${SERVICE_KEY}" ]; then
      SERVICE_KEY="${REPO_ROOT}/${SERVICE_KEY}"
    fi
  fi

  if [ ! -f "${SERVICE_KEY}" ]; then
    echo "[bootstrap_bq_auth] Service account key not found: ${SERVICE_KEY}"
    exit 1
  fi

  export GOOGLE_APPLICATION_CREDENTIALS="${SERVICE_KEY}"
  gcloud auth activate-service-account --key-file "${SERVICE_KEY}" --quiet >/dev/null
fi

if [ -z "${PROJECT_ID}" ]; then
  PROJECT_ID="${BQ_PROJECT_ID:-}"
fi

if [ -n "${PROJECT_ID}" ]; then
  gcloud config set project "${PROJECT_ID}" >/dev/null
fi

ACTIVE_ACCOUNT="$(gcloud config get-value account 2>/dev/null | tr -d '\r' || true)"
if [ "${ACTIVE_ACCOUNT}" = "(unset)" ]; then
  ACTIVE_ACCOUNT=""
fi
if [ -z "${ACTIVE_ACCOUNT}" ]; then
  echo "[bootstrap_bq_auth] No active gcloud account."
  echo "Run once: gcloud auth login --no-launch-browser"
  exit 1
fi

echo "[bootstrap_bq_auth] account=${ACTIVE_ACCOUNT}"
if [ -n "${PROJECT_ID}" ]; then
  echo "[bootstrap_bq_auth] project=${PROJECT_ID}"
fi
