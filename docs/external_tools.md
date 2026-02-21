# External Tools Workflow

## Aktueller Status in dieser Umgebung

- `databricks` CLI: nicht installiert
- `bq` CLI: nicht installiert
- `gcloud` CLI: nicht installiert
- `pwsh` (PowerShell): nicht installiert

## Databricks (CLI)

Ziel: Jobs fuer Bronze/Silver/Gold aus dem Repo steuern.

Empfohlener Ablauf:
1) Databricks CLI installieren
2) Auth konfigurieren (`databricks auth login`)
3) Workspace/Jobs per `databricks.yml` oder JSON definieren
4) Jobs via Skript triggern (z.B. `scripts/run_databricks_jobs.sh`)

## BigQuery (CLI)

Ziel: RAW/CORE/MARTS Uploads und SQL-Transformationen automatisieren.

Empfohlener Ablauf:
1) Google Cloud SDK installieren (`gcloud`, `bq`)
2) Auth konfigurieren (`gcloud auth login` und/oder Service Account)
3) Default Project setzen
4) SQL/JOBS aus `bigquery/` per Skript ausfuehren

## Power BI

### Was direkt automatisierbar ist
- Service-seitig per Power BI REST API (Datasets, Refresh, Deployment)
- Modell-/Berichtsdefinitionen als Dateien ueber PBIP/TMDL im Repo versionieren

### Was nicht direkt automatisierbar ist
- Power BI Desktop selbst hat keinen vollwertigen offiziellen CLI-Workflow,
  mit dem man die GUI hier fernsteuern kann.

## Empfohlener Team-Workflow fuer dieses Projekt

1) Semantik im Repo pflegen (TMDL, Power Query M, DAX)
2) Desktop fuer Layout/Visual-Feinschliff verwenden
3) Service-Deploy und Refresh ueber API/Skripte automatisieren
4) Jede fachliche Aenderung als Commit + Push festhalten
