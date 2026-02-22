# ------------------------------------
# start_db.ps1
#
# Startet die lokale PostgreSQL-Instanz fuer History-ETL.
#
# Usage
# ------------------------------------
# - .\scripts\start_db.ps1
# - .\scripts\start_db.ps1 -WithUi
# ------------------------------------

param(
    [switch]$WithUi
)

$ErrorActionPreference = "Stop"

if ($WithUi) {
    docker compose --profile ui up -d
} else {
    docker compose up -d postgres
}

docker compose ps
