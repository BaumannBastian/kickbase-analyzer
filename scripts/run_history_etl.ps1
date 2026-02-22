# ------------------------------------
# run_history_etl.ps1
#
# Startet den lokalen Postgres-History-ETL mit CLI-Parametern.
#
# Usage
# ------------------------------------
# - .\scripts\run_history_etl.ps1 --player-name-like orban --max-players 1
# ------------------------------------

param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CliArgs
)

$ErrorActionPreference = "Stop"
python -m src.etl_history @CliArgs
