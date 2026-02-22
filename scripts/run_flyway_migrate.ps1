# ------------------------------------
# run_flyway_migrate.ps1
#
# Fuehrt Flyway-Migrationen fuer die lokale History-DB aus.
#
# Usage
# ------------------------------------
# - .\scripts\run_flyway_migrate.ps1
# ------------------------------------

$ErrorActionPreference = "Stop"

$flywayCmd = Get-Command flyway -ErrorAction SilentlyContinue
if ($flywayCmd) {
    flyway -configFiles=flyway.conf migrate
    exit 0
}

Write-Host "flyway binary not found. Falling back to dockerized flyway..."
$pgHost = if ($env:PGHOST) { $env:PGHOST } else { "postgres" }
$pgPort = if ($env:PGPORT) { $env:PGPORT } else { "5432" }
$pgDatabase = if ($env:PGDATABASE) { $env:PGDATABASE } else { "kickbase_history" }
$pgUser = if ($env:PGUSER) { $env:PGUSER } else { "kickbase" }
$pgPassword = if ($env:PGPASSWORD) { $env:PGPASSWORD } else { "kickbase" }

docker run --rm `
  --network kickbaseanalyzer_default `
  -e PGHOST=$pgHost `
  -e PGPORT=$pgPort `
  -e PGDATABASE=$pgDatabase `
  -e PGUSER=$pgUser `
  -e PGPASSWORD=$pgPassword `
  -v "${PWD}\sql:/flyway/sql" `
  -v "${PWD}\flyway.conf:/flyway/conf/flyway.conf" `
  redgate/flyway:10 `
  -configFiles=/flyway/conf/flyway.conf migrate
