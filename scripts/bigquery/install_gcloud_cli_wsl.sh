#!/usr/bin/env bash
# ------------------------------------
# install_gcloud_cli_wsl.sh
#
# Installiert die Google Cloud CLI (gcloud + bq) im User-Home
# fuer WSL/Linux ohne globale Systeminstallation.
#
# Usage
# ------------------------------------
# - ./scripts/bigquery/install_gcloud_cli_wsl.sh
# - GCLOUD_VERSION=510.0.0 ./scripts/bigquery/install_gcloud_cli_wsl.sh
# ------------------------------------

set -euo pipefail

GCLOUD_VERSION="${GCLOUD_VERSION:-510.0.0}"
ARCHIVE_NAME="google-cloud-cli-${GCLOUD_VERSION}-linux-x86_64.tar.gz"
DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${ARCHIVE_NAME}"
INSTALL_ROOT="${HOME}/tools"
SDK_DIR="${INSTALL_ROOT}/google-cloud-sdk"
ARCHIVE_PATH="${INSTALL_ROOT}/${ARCHIVE_NAME}"

mkdir -p "${INSTALL_ROOT}"
mkdir -p "${HOME}/bin"

if [ ! -d "${SDK_DIR}" ]; then
  echo "Downloading ${DOWNLOAD_URL}"
  curl -fL "${DOWNLOAD_URL}" -o "${ARCHIVE_PATH}"
  tar -xzf "${ARCHIVE_PATH}" -C "${INSTALL_ROOT}"
fi

"${SDK_DIR}/install.sh" \
  --quiet \
  --usage-reporting=false \
  --bash-completion=false \
  --path-update=false \
  --rc-path=/dev/null

ln -sf "${SDK_DIR}/bin/gcloud" "${HOME}/bin/gcloud"
ln -sf "${SDK_DIR}/bin/bq" "${HOME}/bin/bq"

echo "Installed:"
echo "  $(command -v gcloud || true)"
echo "  $(command -v bq || true)"
echo "If needed: export PATH=\"${HOME}/bin:\$PATH\""
