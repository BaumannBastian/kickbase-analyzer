# Databricks + BigQuery Setup (Kickbase Analyzer)

Diese Anleitung folgt dem Muster aus `house-price-model`:

1) Databricks Repo syncen
2) Databricks Jobs (Bronze -> Silver -> Gold) ausfuehren
3) Gold-Output nach BigQuery RAW laden
4) CORE/MARTS Views anwenden

## 1) Databricks CLI Setup (Windows + WSL)

### Voraussetzungen
- Databricks Workspace Access
- PAT fuer Databricks oder OAuth Login
- In dieser Session laeuft der Agent in WSL2 Linux (`bash`)

### Option A (empfohlen): Databricks CLI in WSL installieren

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Danach in PATH aufnehmen, falls noetig:

```bash
export PATH="$HOME/.databricks/bin:$PATH"
```

Auth einrichten:

```bash
databricks auth login --host "https://<dein-workspace-host>.cloud.databricks.com"
```

Wichtig: Host ohne `/browse?...` und ohne Query-Parameter eingeben.

Profile pruefen:

```bash
databricks auth profiles
```

### Option B: Windows CLI weiterverwenden
Wenn du weiterhin nur `databricks.exe` auf Windows nutzt, starte die Databricks-Schritte in PowerShell.

## 2) Databricks Repo und Job-Verknuepfung

### Platzhalter
- `<DATABRICKS_PROFILE>`
- `<DATABRICKS_REPO_PATH>` z.B. `/Repos/basti.baumann@gmx.net/kickbase-analyzer`
- `<GIT_REPO_URL>` = `https://github.com/BaumannBastian/kickbase-analyzer.git`

### 2.1 Repo in Databricks anlegen

```bash
databricks repos create "https://github.com/BaumannBastian/kickbase-analyzer.git" gitHub --path "/Repos/<user>/kickbase-analyzer" --profile "<DATABRICKS_PROFILE>"
```

### 2.2 Jobs einmalig auf Repo-Notebook-Pfade umstellen

```bash
python3 -m scripts.databricks.update_jobs_to_repo \
  --repo-path "/Repos/<user>/kickbase-analyzer" \
  --profile "<DATABRICKS_PROFILE>" \
  --job-ids "<bronze_job_id>,<silver_job_id>,<gold_job_id>"
```

Alternative: Jobs direkt anlegen oder idempotent aktualisieren:

```bash
python3 -m scripts.databricks.create_lakehouse_jobs \
  --repo-path "/Repos/<user>/kickbase-analyzer" \
  --profile "<DATABRICKS_PROFILE>"
```

Das Skript gibt am Ende `DATABRICKS_JOB_ID_BRONZE`, `..._SILVER`, `..._GOLD` aus.

## 3) Regelmaessiger Databricks Sync + Job Run

### 3.1 Repo syncen

```bash
python3 -m scripts.databricks.sync_repo \
  --repo-path "/Repos/<user>/kickbase-analyzer" \
  --branch main \
  --profile "<DATABRICKS_PROFILE>"
```

### 3.2 Jobs triggern

Variante A (env vars):

```bash
export DATABRICKS_JOB_ID_BRONZE=<bronze_job_id>
export DATABRICKS_JOB_ID_SILVER=<silver_job_id>
export DATABRICKS_JOB_ID_GOLD=<gold_job_id>

python3 -m scripts.databricks.run_lakehouse_jobs --stage all --profile "<DATABRICKS_PROFILE>"
```

Variante B (direkt per Argumente):

```bash
python3 -m scripts.databricks.run_lakehouse_jobs \
  --stage all \
  --profile "<DATABRICKS_PROFILE>" \
  --job-id-bronze <bronze_job_id> \
  --job-id-silver <silver_job_id> \
  --job-id-gold <gold_job_id>
```

## 4) BigQuery CLI Setup

### 4.1 Google Cloud CLI installieren
Siehe offizielles Install-HowTo fuer dein OS.

Hinweis fuer WSL: Eine Installation in Windows PowerShell reicht in WSL nicht.
Wenn du die Skripte in WSL startest, muessen `gcloud` und `bq` auch in WSL installiert sein.

Ubuntu/WSL Beispiel:

```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates gnupg curl
curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list >/dev/null
sudo apt-get update
sudo apt-get install -y google-cloud-cli
```

### 4.2 Auth setzen
Interaktiv:

```bash
gcloud auth login
gcloud config set project <gcp_project_id>
```

Service Account (CI/Automation):

```bash
gcloud auth activate-service-account --key-file /path/to/service_account.json
gcloud config set project <gcp_project_id>
```

### 4.3 bq pruefen

```bash
bq version
```

## 5) Kickbase Output nach BigQuery laden

### 5.1 Gold -> RAW Export (lokal)

```bash
./scripts/bigquery/run_prepare_raw_exports.sh
```

### 5.2 RAW in BigQuery laden

```bash
./scripts/bigquery/run_load_raw_bq.sh --project <gcp_project_id> --dataset kickbase_raw
```

### 5.3 CORE/MARTS Views anwenden

```bash
./scripts/bigquery/run_apply_views_bq.sh --project <gcp_project_id> --raw kickbase_raw --core kickbase_core --marts kickbase_marts
```

### 5.4 End-to-End BigQuery Pipeline

```bash
./scripts/bigquery/run_bigquery_pipeline.sh --project <gcp_project_id>
```
