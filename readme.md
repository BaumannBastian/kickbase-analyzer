# README.md

# Kickbase Analyzer

Privates End-to-End Projekt fuer Kickbase-Analysen:

Local Ingestion (Kickbase API + LigaInsider + Odds API)
-> Databricks Lakehouse (Bronze/Silver/Gold)
-> BigQuery (RAW/CORE/MARTS)
-> Power BI Desktop

## Status (2026-02-22)

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
- `docs/setup_postgres_history.md`

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

## PostgreSQL History (Docker + Flyway + Python ETL)

Ziel:
- komplette Historie pro Spieler in lokaler Postgres DB
- inkrementelle Updates ohne Duplikate
- zuerst kontrolliert mit Einzelspieler testen (`--max-players 1`)

### Setup (einmalig)

1) Abhaengigkeiten installieren

```powershell
python -m pip install -r requirements.txt
```

2) `.env` um Postgres/Flyway/Databricks-Werte ergaenzen (Vorlage in `.env.example`)

3) Datenbank starten

```powershell
docker compose up -d postgres
docker compose ps
```

4) Schema migrieren

```powershell
./scripts/run_flyway_migrate.sh
```

### History-ETL laufen lassen

Einzelspieler-Test (sicherer Start):

```powershell
python -m src.etl_history --player-name-like orban --max-players 1 --timeframe-days 3650 --days-from 1 --save-raw
```

Mehr Spieler:

```powershell
python -m src.etl_history --max-players 25 --timeframe-days 3650 --days-from 1
```

CSV-Fallback ohne Databricks Driver:

```powershell
python -m src.etl_history --players-csv .\\in\\players.csv --max-players 5 --days-from 1 --days-to 3
```

### Wichtige Tabellen im lokalen Postgres

Schema: `kickbase_raw`

- `dim_player`
- `dim_event_type`
- `dim_team`
- `dim_match`
- `fact_market_value_daily`
- `fact_player_match`
- `fact_player_event`
- `etl_state`

Wichtige Felder:
- `dim_player.player_uid` (interner stabiler Schluessel)
- `dim_player.kb_player_id` als explizite Kickbase-Referenz-ID
- `dim_player.image_blob` (`BYTEA`) + `image_mime` + `image_sha256` fuer Spielerbilder
- `dim_team.team_code` + `dim_team.team_name` im Format `RBL (RB Leipzig)`
- `fact_market_value_daily.mv_date`, `fact_market_value_daily.market_value`
- `fact_player_match.match_uid` im kompakten Format `25/26-MD23-RBLBVB`
- `fact_player_match.is_home` (`true`/`false`) und `fact_player_match.match_result` (`W`/`D`/`L`)
- `fact_player_event.event_type_id` + Join auf `dim_event_type.event_name` fuer lesbare Eventnamen

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
- [x] PostgreSQL History Smoke-Test mit Orban erfolgreich (Market Value + Performance + Event-Breakdown)
- [x] Eventtype-Mapping korrigiert (`/v4/live/eventtypes` mit `i/ti`) und in `dim_event_type` geladen
- [x] Performance um `is_home` + `match_result` (`W`/`D`/`L`) erweitert
- [x] Legacy-Events mit `season_label='unknown'` pro Spieler-Lauf automatisch bereinigt
- [x] History-Schema auf `kb_player_id` umgestellt (klar getrennt von internem `player_uid`)
- [x] Kompakte `match_uid` eingefuehrt (`25/26-MD23-RBLBVB` statt Timestamp/Team-ID-Kombination)
- [x] Teamnamen auf Anzeigeformat `RBL (RB Leipzig)` standardisiert und Bildspeicherung auf `BYTEA` umgestellt

Offen fuer morgen (V0.9):
- [ ] Kickbase-Ingestion-Frequenzen entkoppeln (Marktwert taeglich, Performance an Spieltagen, Status/Lineup intraday).
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
