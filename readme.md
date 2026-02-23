# README.md

# Kickbase Analyzer

Privates End-to-End Projekt fuer Kickbase-Analysen:

Local Ingestion (Kickbase API + LigaInsider + Odds API)
-> Databricks Lakehouse (Bronze/Silver/Gold)
-> BigQuery (RAW/CORE/MARTS)
-> Power BI Desktop

## Status (2026-02-23)

Der Bronze-Layer ist funktional:
- `kickbase_player_snapshot`
- `ligainsider_status_snapshot`
- `odds_match_snapshot`

LigaInsider-Snapshots sind erweitert:
- Team-Aufstellungsseiten (`/<team>/<id>/`) fuer Lineup/Positionskonkurrenz
- Team-Kaderseiten (`/<team>/<id>/kader/`) fuer vollere Spielerabdeckung inkl. LI-ID, Birthdate, Bild-Quelle

PostgreSQL History ist aktiv:
- source-unabhaengiger `player_uid` in allen spielerbezogenen Facts
- `team_uid` als sprechender Text-Key (z.B. `RBL`, `BVB`)
- Spielerbilder als `BYTEA` in `dim_player`
- Identity-Merge fuer `player_uid`-Transitions (z.B. wenn Birthdate spaeter verfuegbar wird)
- Competition-Scope-Cleanup aktiv: `dim_team` + Match/Facts werden auf Bundesliga (`competition_id=1`) begrenzt

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
- `docs/ml_workflow.md`

## Quick Start (PowerShell)

```powershell
cd "C:\Users\basti\Documents\Kickbase Analyzer"
python -m pip install -e .
python -m pip install -r requirements-ml.txt
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

Driver-CSV aus Kickbase-Bronze erzeugen (fuer History-Batches):

```powershell
python -m scripts.history.build_players_csv_from_bronze --input-dir data/bronze --output in/players_all.csv
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

Batch-Load (kontrolliert, mit Offset):

```powershell
python -m scripts.history.run_history_batches --env-file .env --players-csv in/players_all.csv --batch-size 50
```

CSV-Fallback ohne Databricks Driver:

```powershell
python -m src.etl_history --players-csv .\\in\\players.csv --max-players 5 --days-from 1 --days-to 3
```

Backfill fehlender Player-Enrichment-Felder + Problemreport:

```powershell
python -m scripts.history.backfill_player_enrichment --env-file .env --report-dir out/reports
```

Raw-Konsistenzcheck (`points_total` vs Event-Summe):

```powershell
python -m scripts.history.check_match_points_consistency --env-file .env --limit 25
python -m scripts.history.check_match_points_consistency --env-file .env --player-name-like amiri
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
- `dim_player.player_name`, `dim_player.player_birthdate`, `dim_player.player_position`
- `dim_player.team_uid` als aktuelles Team-Mapping am Spielerstammsatz
- `dim_player.kb_player_id` als explizite Kickbase-Referenz-ID
- `dim_player.image_blob` (`BYTEA`) + `image_mime` + `image_sha256` + `image_bytes` fuer Spielerbilder
- `dim_player.image_local_path` fuer den lokalen Datei-Link (`data/history/player_images/<player_uid>.jpg`)
- `dim_player.ligainsider_name` + `dim_player.ligainsider_profile_url`
- `dim_team.team_uid` + `dim_team.team_name` + `dim_team.kickbase_team_id` + `dim_team.ligainsider_team_url`
- `dim_team.team_uid` ist auf stabile Teamkuerzel normalisiert (keine Legacy-`Txx` Codes mehr)
- Scope-Guard: nur Bundesliga-Teams bleiben in `dim_team`; `dim_match`/`fact_player_match` enthalten nur Bundesliga-Matches
- `fact_market_value_daily.mv_date`, `fact_market_value_daily.market_value`
- `fact_player_match.match_uid` im kompakten Format `25/26-MD23-RBLBVB`
- `fact_player_match.is_home` (`true`/`false`) und `fact_player_match.match_result` (`W`/`D`/`L`)
- `fact_player_event.event_type_id` + Join auf `dim_event_type.event_name` fuer lesbare Eventnamen
- Retention: `dim_match`, `fact_player_match`, `fact_player_event` und `fact_market_value_daily` werden auf akt. Saison + 2 vorherige begrenzt.

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

Selektive History fuer Player-Profile (Postgres + Bronze -> BQ):

```powershell
./scripts/bigquery/run_prepare_history_exports.sh --env-file .env
./scripts/bigquery/run_load_history_bq.sh --project <gcp_project_id>
./scripts/bigquery/run_apply_history_views_bq.sh --project <gcp_project_id>
```

Oder als End-to-End Lauf:

```powershell
./scripts/bigquery/run_history_bigquery_pipeline.sh --env-file .env --project <gcp_project_id>
```

ML -> BigQuery RAW + ML Views:

```powershell
./scripts/bigquery/run_ml_bigquery_pipeline.sh --env-file .env --project <gcp_project_id>
./scripts/bigquery/run_apply_ml_views_bq.sh --project <gcp_project_id>
```

Power BI Desktop Asset Pack:

```powershell
python -m scripts.powerbi_desktop.export_desktop_assets --project <gcp_project_id>
```

## ML Training (historical CV)

Trainingspfad:
- `silver -> gold -> lokal (ML) -> BigQuery RAW -> BigQuery CORE -> BigQuery MARTS -> Power BI`
- Kein Rueckschreiben von lokal nach Databricks Gold.

Training + CV starten:

```powershell
python -m scripts.ml.train_hierarchical_models --env-file .env --cv-splits 4 --validation-days 21
```

Nur sklearn (ohne Torch):

```powershell
python -m scripts.ml.train_hierarchical_models --env-file .env --skip-torch
```

End-to-end ML -> BigQuery RAW (Champion inklusive):

```powershell
python -m scripts.ml.run_ml_bigquery_pipeline --env-file .env --project <gcp_project_id>
```

Scheduler fuer regelmaessige CV-Runs:

```powershell
python -m scripts.ml.run_ml_pipeline_scheduler --interval-seconds 21600 -- --env-file .env --project <gcp_project_id>
```

Outputs pro Lauf:
- `data/ml_runs/<run_ts>/cv_sklearn_folds.csv`
- `data/ml_runs/<run_ts>/cv_sklearn_summary.json`
- `data/ml_runs/<run_ts>/live_predictions_sklearn.csv`
- `data/ml_runs/<run_ts>/live_predictions_champion.csv`
- `data/ml_runs/<run_ts>/champion_selection.json`
- optional Torch: `cv_torch_folds.csv`, `cv_torch_summary.json`, `live_predictions_torch.csv`
- `data/ml_runs/<run_ts>/run_summary.json`

BigQuery ML RAW Export-Dateien:
- `data/warehouse/raw_ml/ml_live_predictions.jsonl`
- `data/warehouse/raw_ml/ml_cv_fold_metrics.jsonl`
- `data/warehouse/raw_ml/ml_champion_selection.jsonl`
- `data/warehouse/raw_ml/ml_run_summary.jsonl`

BigQuery History RAW Export-Dateien:
- `data/warehouse/raw_history/hist_player_profile_snapshot.jsonl`
- `data/warehouse/raw_history/hist_team_snapshot.jsonl`
- `data/warehouse/raw_history/hist_player_marketvalue_daily.jsonl`
- `data/warehouse/raw_history/hist_player_match_summary.jsonl`
- `data/warehouse/raw_history/hist_player_match_components.jsonl`
- `data/warehouse/raw_history/hist_team_lineup_players.jsonl`
- `data/warehouse/raw_history/hist_team_odds_snapshot.jsonl`

History MARTS Views (Power BI):
- `mart_hist_player_profile`
- `mart_hist_player_marketvalue_curve`
- `mart_hist_player_match_breakdown`
- `mart_hist_player_comparison`
- `mart_hist_team_outlook`

## Roadmap (Kurz)

- Bronze stabil und quellenweise steuerbar halten.
- History-Load in kontrollierten Batches skalieren (5 -> 50 -> Full Roster).
- Silver v0.9:
  - `silver.player_snapshot` (Kickbase + LigaInsider Join, eine Zeile je Spieler/Snapshot)
  - `silver.team_matchup_snapshot` (naechstes Matchup, Odds-Features, wahrscheinliche Aufstellung)
- Gold v1.0:
  - Hierarchische ML-Modelle (sklearn + PyTorch) auf gemeinsamem Preprocessing
  - Feature Engineering fuer Start/Sub/Minuten/Rohpunkte/On-Top-Punkte
  - Modellierung + Kalibrierung + Backtesting-Haertung

## To-do morgen (2026-02-23)

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
- [x] LigaInsider-Enrichment gehaertet (Name/Slug/Last-Name-Fallback), Birthdates fuer 5er-Testload gezogen und `player_uid`-Merges auf `YYYYMMDD` validiert
- [x] Bildpfad in DB auf lokalen Dateipfad umgestellt (`dim_player.image_local_path`) statt externer URL
- [x] LigaInsider-Ingestion auf Team-Kaderseiten erweitert (`.../kader/`), dadurch vollere LI-Spielerabdeckung in Bronze
- [x] History-ETL um Batch-Offset erweitert (`--player-offset`) und Batch-Runner-Skript ergaenzt
- [x] Konsistenzcheck-Skript fuer `fact_player_match` vs `fact_player_event` hinzugefuegt
- [x] Full-Roster-Historyload in 50er-Batches abgeschlossen (`337/337` Driver-Zeilen verarbeitet)
- [x] Event-Dedupe gehaertet (stabile API-Event-ID bevorzugt) und bestehende Duplikate bereinigt
- [x] `dim_team.ligainsider_team_url` auf Team-Root-URLs normalisiert (kein `/kader/`-Mix)
- [x] Artefakt-Cleanup: lokale Batch-CSV-Artefakte aus Git entfernt (`in/players_5.csv`, `in/players_all.csv` ignored)

Offen fuer morgen (V0.9):
- [ ] Kickbase-Ingestion-Frequenzen entkoppeln (Marktwert taeglich, Performance an Spieltagen, Status/Lineup intraday).
- [ ] Bronze QA: Teamnamen-Normalisierung Odds -> Club-Mapping vorbereiten
- [x] BigQuery-Rolle final festgezogen: Reporting-Layer basiert auf Databricks Gold + ML-Outputs; History nur selektiv nach BI-Use-Case.
- [x] BigQuery-Loadpfad fuer History definiert: kein blindes RAW_HISTORY Full-Dump; initial nur Gold + ML in BigQuery.
- [x] Bildstrategie dokumentiert: `image_blob` bleibt lokal in Postgres; in BigQuery nur Metadaten (`player_uid`, `image_mime`, `image_sha256`, `image_local_path`).
- [ ] Event-Parser fuer Sonderfaelle haerten (`event_points_total=0` bei vorhandenen Match-Punkten) und Datenquelle gegen Kickbase-UI gegentesten
- [ ] Konsistenzcheck als festen Gate-Step in den Batch-Runner integrieren (Warnung/Abbruch bei groesseren Abweichungen)
- [x] Silver Tabelle `player_snapshot` gebaut (Kickbase + LigaInsider in einer Zeile pro Spieler/Snapshot)
- [x] Silver Tabelle `team_matchup_snapshot` gebaut (Odds + wahrscheinliche Aufstellung pro Team)
- [ ] Persistente `player_uid`-Vergabe mit Mapping-Historie aufsetzen
- [ ] Databricks Bronze Job so erweitern, dass Odds-only Runs ohne KB/LI-Timestamp-Konflikt ingestiert werden
- [ ] Erste Gold-Spezifikation fuer drei Targets finalisieren (spielt, punkte, marktwert)
- [ ] LigaInsider-Restfaelle (ohne Match im Snapshot) mit Search/Profile-Fallback weiter reduzieren; offene Faelle bleiben im JSONL-Report dokumentiert

## Tests

```powershell
./scripts/lint.sh
./scripts/test.sh
```
