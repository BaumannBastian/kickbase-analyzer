# External Tools Workflow

## Databricks (CLI)

Ziel: Bronze/Silver/Gold Jobs aus dem Repo synchronisieren und ausfuehren.

Kernkommandos:
1) Repo synchronisieren  
`python3 -m scripts.databricks.sync_repo --repo-path "/Repos/<user>/kickbase-analyzer" --branch main --profile "<profile>"`
2) Jobs anlegen/aktualisieren  
`python3 -m scripts.databricks.create_lakehouse_jobs --repo-path "/Repos/<user>/kickbase-analyzer" --profile "<profile>"`
3) Jobs ausfuehren  
`python3 -m scripts.databricks.run_lakehouse_jobs --stage all --profile "<profile>" --job-id-bronze <id> --job-id-silver <id> --job-id-gold <id>`

## BigQuery (CLI)

Ziel: RAW/CORE/MARTS Uploads und SQL-Transformationen automatisieren.

Kernkommandos:
1) CLI installieren (WSL/Linux)  
`./scripts/bigquery/install_gcloud_cli_wsl.sh`
2) Auth + Projekt setzen  
`./scripts/bigquery/configure_gcloud_auth.sh --project <gcp_project_id>`
3) Setup pruefen  
`./scripts/bigquery/check_bq_setup.sh`
4) Pipeline laufen lassen  
`./scripts/bigquery/run_bigquery_pipeline.sh --project <gcp_project_id>`

## Power BI Desktop (lokal)

Ziel: Dashboards lokal entwickeln, basierend auf BigQuery MARTS, ohne Service API.

Voraussetzungen:
1) Power BI Desktop lokal installiert
2) BigQuery Zugriff fuer das genutzte Google-Konto
3) BigQuery MARTS gebaut (`kickbase_marts`)

Kernkommandos:
1) M/DAX/TMDL Asset-Pack exportieren  
`python -m scripts.powerbi_desktop.export_desktop_assets --project <gcp_project_id>`
2) Detaillierte Desktop-Anleitung  
`docs/setup_powerbi_desktop.md`

## Quellen-Setup (Kickbase + LigaInsider)

Kickbase Auth pruefen:
`./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots`

Private Ingestion mit LigaInsider URL:
`./scripts/run_private_ingestion.sh --env-file .env`

Relevante Variablen:
- `KICKBASE_*` fuer Auth + API Pfade
- `LIGAINSIDER_STATUS_URL` fuer live scrape
- `LIGAINSIDER_STATUS_FILE` als optionaler Fallback
