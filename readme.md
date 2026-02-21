# README.md

# Kickbase Analyzer

Privates End-to-End Projekt fuer Kickbase-Analysen:

Local Ingestion (Kickbase API + LigaInsider + Odds API)
-> Databricks Lakehouse (Bronze/Silver/Gold)
-> BigQuery (RAW/CORE/MARTS)
-> Power BI Desktop

## Status (2026-02-21)

Der Bronze-Layer ist funktional:
- `kickbase_player_snapshot`
- `ligainsider_status_snapshot`
- `odds_match_snapshot`

Private Ingestion kann jede Quelle einzeln laden:
- `--sources kickbase`
- `--sources ligainsider`
- `--sources odds`
- `--sources all`

CI ist auf `bash`-Aufruf der Skripte umgestellt, damit keine Execute-Bit-Probleme mehr auftreten.

## Wichtige Hinweise

- Projekt ist fuer private Nutzung gedacht.
- Secrets bleiben lokal in `.env` und werden nie committed.
- LigaInsider nur privat/schonend nutzen (Rate-Limit + Cache).

## Repo-Dokumente

- `docs/architecture.md`
- `docs/data_contract.md`
- `docs/compliance.md`
- `docs/setup_sources_private.md`
- `docs/setup_databricks_bigquery.md`
- `docs/setup_powerbi_desktop.md`

## Quick Start (PowerShell)

```powershell
cd "C:\Users\basti\Documents\Kickbase Analyzer"
python -m pip install -e .
```

### Connectivity Checks

```powershell
python -m scripts.auth.check_kickbase_auth --env-file .env --verify-snapshots
python -m scripts.auth.discover_kickbase_leagues --env-file .env
python -m scripts.auth.check_ligainsider_scrape --env-file .env
```

### Ingestion-Modi

Nur Kickbase:

```powershell
python -m local_ingestion.runners.run_ingestion --mode private --sources kickbase --env-file .env
```

Nur LigaInsider:

```powershell
python -m local_ingestion.runners.run_ingestion --mode private --sources ligainsider --env-file .env
```

Nur Odds:

```powershell
python -m local_ingestion.runners.run_ingestion --mode private --sources odds --env-file .env
```

Alle drei Quellen:

```powershell
python -m local_ingestion.runners.run_ingestion --mode private --sources all --env-file .env
```

Bronze-Tabellen anzeigen:

```powershell
python -m scripts.analysis.show_bronze_tables --input-dir data/bronze --limit 20
```

## Bronze Outputs

Pro Run entstehen NDJSON-Dateien in `data/bronze/`:
- `kickbase_player_snapshot_<timestamp>.ndjson`
- `ligainsider_status_snapshot_<timestamp>.ndjson`
- `odds_match_snapshot_<timestamp>.ndjson`
- `ingestion_runs.ndjson`

## Databricks / BigQuery / Power BI

Databricks Jobs lokal simulieren:

```powershell
./scripts/run_databricks_jobs_demo.sh
```

BigQuery Pipeline:

```powershell
./scripts/bigquery/run_prepare_raw_exports.sh
./scripts/bigquery/run_load_raw_bq.sh --project <gcp_project_id>
./scripts/bigquery/run_apply_views_bq.sh --project <gcp_project_id>
```

Power BI Desktop Asset Pack:

```powershell
python -m scripts.powerbi_desktop.export_desktop_assets --project <gcp_project_id>
```

## Roadmap (Kurz)

- Bronze stabil und quellenweise steuerbar halten.
- Silver v0.9:
  - `silver.player_snapshot` (Kickbase + LigaInsider Join, eine Zeile je Spieler/Snapshot)
  - `silver.team_matchup_snapshot` (naechstes Matchup, Odds-Features, Formkurve)
- Gold v1.0:
  - Features fuer Startet/Punkte/Marktwert in getrennten, klaren Feature-Sets
  - Modellierung + Kalibrierung + Backtesting-Haertung

## Morgen To-do (2026-02-22)

Erledigt heute:
- [x] Selektive Ingestion pro Quelle (`kickbase`, `ligainsider`, `odds`, `all`)
- [x] Odds API als dritte Bronze-Quelle eingebunden
- [x] Bronze Viewer auf getrennte Latest-Timestamps pro Tabelle umgestellt
- [x] CI-Fix fuer `Permission denied` bei Shell-Skripten
- [x] README bereinigt (keine fehlerhaften ChatGPT-Referenzreste)

Offen fuer morgen (V0.9):
- [ ] Bronze QA: Teamnamen-Normalisierung Odds -> Club-Mapping vorbereiten
- [ ] Silver Tabelle `player_snapshot` bauen (nur Sammeln/Joinen, noch keine Modelllogik)
- [ ] Silver Tabelle `team_matchup_snapshot` bauen
- [ ] Persistente `player_uid`-Vergabe mit Mapping-Historie aufsetzen
- [ ] Databricks Bronze Job so erweitern, dass Odds-only Runs ohne KB/LI-Timestamp-Konflikt ingestiert werden
- [ ] Erste Gold-Spezifikation fuer drei Targets finalisieren (spielt, punkte, marktwert)

## Tests

```powershell
./scripts/lint.sh
./scripts/test.sh
```
