# docs/architecture.md

# Kickbase Analyzer – Architecture (Local → Databricks Lakehouse → Local ML → BigQuery DWH → Power BI)

## 0) Ziel
Ein **privates** Analyse- und Prognose-System für Kickbase-Entscheidungen (Transfers/Lineup/Trading), das Daten aus mehreren Quellen sammelt, vereinheitlicht, historisiert und daraus **erklärbare** Prognosen + Rankings baut.

**Kern-Outputs pro Spieler**
- StartProbability
- ExpectedPointsNextMatchday (inkl. Breakdown)
- ExpectedPointsRestOfSeason
- ExpectedMarketValueNextMatchday
- ExpectedMarketValueChange7d
- Risiko/Varianz (StdDev, P10/P50/P90, P(DNP), Injury/Rotation Flags)
- Overall Rating / Value Score (Preis vs Expected Outputs, risikoadjustiert)

**Primäre Konsumenten**
- Power BI Dashboard (aus BigQuery MARTS)
- Optional später: Streamlit/Read-Only API für private Nutzung

---

## 1) Non-Goals (MVP)
- Kein öffentlicher Service (kein “Kickbase Analyzer as a Service”)
- Kein aggressives Crawling/Scraping
- Kein “Deep Learning first” → erst Baselines + Backtesting, dann ML-Upgrade

---

## 2) Recht / ToS / Compliance (Portfolio-sicheres Design)
Hinweis: Keine Rechtsberatung. Ziel ist ein risikoarmes Portfolio-Setup.

### 2.1 LigaInsider
- Nutzung ist laut AGB auf **private Zwecke** beschränkt.
Konsequenz:
- Scraping nur im **Private Mode** (lokal, mit Cache + Rate-Limit)
- Für GitHub/Recruiter: **Demo Mode ohne LigaInsider-Daten** (synthetisch / selbst generiert)

### 2.2 Kickbase
- Terms betonen “personal use” und untersagen kommerzielle Nutzung/Resale; technische Hilfsmittel, die Vorteile verschaffen, sind problematisch.
Konsequenz:
- Private Mode: Nutzung mit eigenem Account, schonend (Rate-Limits, Caching)
- Demo Mode: keine echten Kickbase-Daten, nur Demo-Datasets

### 2.3 Databricks Free Edition (Egress)
- Outbound-Internet kann eingeschränkt sein. Deshalb laufen Scrapes/API Calls bewusst **lokal**.
Konsequenz:
- Ingestion lokal
- Databricks nur für Bronze/Silver/Gold Verarbeitung

---

## 3) End-to-End Dataflow (High Level)

### Step A — Local Ingestion (Laptop/PC)
1) Kickbase API pull (REST, auth token)
2) LigaInsider scrape (BeautifulSoup, HTML → parser)
3) Optional: weitere öffentliche Fußball-Datenquellen (später)

Output: **Raw Staging Files** (JSON/NDJSON/Parquet), versioniert + timestamped:
- kickbase_player_snapshot_YYYY-MM-DDTHHMMSS.json
- ligainsider_status_snapshot_YYYY-MM-DDTHHMMSS.json
- odds_match_snapshot_YYYY-MM-DDTHHMMSS.json (The Odds API, optional)

### Step B — Databricks Lakehouse: Bronze → Silver → Gold
- Bronze: raw ingest (append-only), minimal schema, audit columns
- Silver: Source Sync + Entity Matching (player_uid), cleaning, dedup, canonicalization
- Gold: analytics-ready tables + feature tables (matchday & daily), explainability tables

Orchestrierung:
- Databricks Jobs (Notebooks oder Python Wheel)
- Ausführung/Trigger via Databricks CLI (als “API” von deinem Rechner)

### Step C — Local ML & Analysis
- Gold-Tabellen werden lokal geladen (Parquet/Delta export oder Connector)
- Training/Backtesting/Inference lokal (scikit-learn / PyTorch)
- Predictions/Components werden lokal als Tabellen exportiert (kein Write-back nach Databricks Gold)

### Step D — BigQuery Data Warehouse: RAW → CORE → MARTS
- Upload lokaler ML-Predictions + ausgewaehlter Gold-Outputs in BigQuery RAW
- Transformation in CORE (typisiert/standardisiert, Dimensions/Facts)
- MARTS: Power-BI-ready Views/Tables (Leaderboard, Breakdown, Risk, MW curves)

### Step E — Power BI Dashboards (privat)
- Power BI liest aus BigQuery MARTS
- Dashboard-Entwicklung lokal in Power BI Desktop (ohne Service-API-Abhaengigkeit)
- Versionierte Assets: M/DAX/TMDL Templates im Repo; lokale PBIX/PBIP Iteration in ignored paths

---

## 4) Bronze / Silver / Gold: klare Verträge

### 4.1 Bronze (Raw, append-only)
Ziel: Nichts verlieren. Alles ist zeitlich nachvollziehbar (Backtesting).
- bronze.kickbase_player_snapshot
- bronze.ligainsider_player_status
- bronze.odds_match_snapshot
- bronze.ingestion_runs (job telemetry)

Common columns:
- ingested_at (timestamp)
- source (kickbase / ligainsider / …)
- raw_payload (json string) ODER parsed fields + raw blob optional
- source_version (parser/client version)

### 4.2 Silver (Sync + Identity + Cleaning)
Ziel: Quellen zusammenbringen.
- silver.player_snapshot (eine Zeile pro Spieler/Snapshot; Kickbase + LigaInsider in einem Datensatz)
- silver.team_matchup_snapshot (eine Zeile pro Team fuer naechstes Matchup; Odds + wahrscheinliche Aufstellung)

Key: player_uid als intern stabiler Identifier.

Identity Strategy:
- deterministic match (Name + Birthdate -> `player_uid`)
- fallback matching (Name/Slug + Teamkontext)
- spaeterer Override-Layer bleibt optional fuer Sonderfaelle

### 4.3 Gold (Analytics/ML-ready)
Ziel: Power-BI-ready + Feature Store.
- gold.feat_player_matchday (ein Row pro Spieler + Spieltag, Features)
- gold.feat_player_daily (ein Row pro Spieler + Tag, Features)
- gold.points_components_matchday (Explainability Breakdown)
- gold.marketvalue_inputs_daily (MW forecast inputs)
- gold.quality_metrics (coverage, missingness)

---

## 5) Model Outputs & Explainability (Dashboard-fähig)

### 5.1 ExpectedPointsNextMatchday = Summe von Komponenten
Wir speichern neben pred_total auch Komponenten (Expected Values) in gold.points_components_matchday.

Beispiele:
- Base/Rohpunkte EV: P(play) * E(raw_points_if_play)
- Scorer EV: P(scorer) * E(scorer_bonus)
- Win EV: P(win) * win_bonus
- Minutes Bonus EV: E(minutes_bonus) oder P(play) * E(minutes_bonus | play)
- Cards/Negatives EV: Summe über Events P(event) * points(event) (negativ)

Wichtig:
- Intern kann Monte-Carlo genutzt werden (Korrelationen sauber).
- Für Power BI liefern wir attribution-friendly Komponenten + optional pred_total_mc.

### 5.2 Risk / Varianz
Outputs pro Spieler + Spieltag:
- stddev_points
- p10_points, p50_points, p90_points
- p_dnp (did not play)
- Flags: injury_uncertain, rotation_risk, competitor_present

### 5.3 Market Value
Outputs:
- expected_marketvalue_next_matchday
- expected_marketvalue_change_7d

Power BI:
- aktueller MW (latest daily snapshot)
- erwartete Curve (x Tage)
- expected deltas (1d / 7d)
- optional “going rate” (z.B. aus Markt/Transfers ableiten)

---

## 6) Scheduling / Refresh Strategy

### 6.1 Kickbase Market Value Snapshot
Plan (Europe/Berlin):
- Local ingestion Job: 22:10 Kickbase snapshot
- Optional Retry: 22:20 falls API wackelt

### 6.2 Lineup/Status Refresh (Kickbase + LigaInsider)
Adaptive Frequenz:
- Normal: alle 3h
- Matchday window:
  - T-24h → T-3h: alle 30min
  - T-3h → Kickoff: alle 10min
  - After kickoff +2h: 1 final snapshot

Local scheduler (cron/Task Scheduler/APScheduler) triggert Local ingestion → schreibt Bronze files → Databricks job runs.

---

## 7) BigQuery DWH (RAW → CORE → MARTS)

### RAW
- 1:1 Upload von ML Outputs + ausgewählten Gold Tables
- Minimal transformations (nur ingestion metadata)
- ML RAW Tabellen:
  - `ml_live_predictions`
  - `ml_cv_fold_metrics`
  - `ml_champion_selection`
  - `ml_run_summary`

### CORE
- typisierte Facts/Dims
- standardisierte Keys (player_uid, team_uid, season, matchday)
- Datums-/Zeitnormierung
- Slowly-changing dims optional
- ML CORE Views:
  - `v_ml_live_predictions`
  - `v_ml_cv_fold_metrics`
  - `v_ml_champion_selection`
  - `v_ml_run_summary`

### MARTS (Power BI ready)
- mart_player_leaderboard (Value Score ranking, filterbar)
- mart_points_breakdown (Komponenten-Table, perfekt für stacked bars)
- mart_risk_overview (p10/p90/stddev/p_dnp)
- mart_marketvalue_dashboard (curve + deltas + current)
- mart_ml_player_predictions (Champion-Output je Spieler fuer naechstes Spiel)
- mart_ml_model_monitoring (CV/Champion Monitoring je ML-Run)

### Serving-Entscheid (final)
- BigQuery ist der zentrale Reporting-Layer fuer Power BI.
- Inputs fuer BigQuery kommen aus:
  - Databricks Gold (aktuelle analytische Features)
  - lokalem ML-Output (Predictions, CV, Champion)
  - selektiver Postgres-History (nur falls fuer Reporting wirklich benoetigt)

### History-Replikation (final)
- Kein blindes 1:1 Kopieren der gesamten Postgres-RAW-History nach BigQuery.
- Selektiver Scope fuer BigQuery:
  - Gold-Outputs (aktuell)
  - ML-Outputs (`ml_*`)
  - aus Postgres nur reduzierte Reporting-History:
    - `hist_player_profile_snapshot`
    - `hist_team_snapshot`
    - `hist_player_marketvalue_daily`
    - `hist_player_match_summary`
    - `hist_player_match_components`
  - aus Bronze nur aktuelle Team-Snapshots:
    - `hist_team_lineup_players`
    - `hist_team_odds_snapshot`
- Postgres bleibt Source-of-Truth fuer komplette RAW-History inkl. Event-Granularitaet;
  BigQuery erhaelt nur Reporting-relevante Teilmengen.

### Binary-Policy (final)
- Spielerbilder (`image_blob`/`BYTEA`) bleiben ausschliesslich lokal in Postgres.
- Nach BigQuery gehen nur Bildmetadaten:
  - `player_uid`
  - `image_mime`
  - `image_sha256`
  - `image_local_path`

---

## 8) Repo-Struktur (ordentlich & Codex-friendly)

kickbase-analyzer/
  README.md
  docs/
    architecture.md
    data_contract.md
    compliance.md

  local_ingestion/
    kickbase_client/
    ligainsider_scraper/
    runners/
    configs/

  databricks/
    jobs/
      bronze_ingest/
      silver_sync/
      gold_features/
    notebooks/
    wheel/                 (optional: packaged code for jobs)

  ml_local/
    training/
    inference/
    backtesting/
    models/

  bigquery/
    raw_load/
    core_transform/
    marts/
    sql/

  dashboards/
    powerbi/
      README.md
      templates/
        bigquery_marts_queries.pq
        measures.dax
        model.tmdl
      local/                (ignored, nicht versioniert)
      screenshots/

  demo/
    data/                  (synthetic/demo datasets)
    scripts/

  scripts/
    setup_env.ps1
    run_local_ingestion.ps1
    run_databricks_jobs.ps1
    run_ml.ps1
    load_bigquery.ps1

  .env.example
  requirements.txt
  pyproject.toml
  .gitignore

---

## 9) Tech Stack

### Local
- Python 3.11+
- requests/httpx + caching
- BeautifulSoup4
- pydantic
- logging (structured)
- Parquet (pyarrow)

### Databricks
- Databricks Jobs + Databricks CLI
- PySpark / Delta Lake
- Bronze/Silver/Gold tables

### ML (local)
- pandas / polars
- scikit-learn (Baselines, Calibration)
- optional PyTorch (NN)

### Warehouse & BI
- BigQuery (RAW/CORE/MARTS)
- SQL transformations (views/tables)
- Power BI (Import aus BigQuery)

---

## 10) To-Do List (Backlog)

### MVP-0 (repo skeleton + contracts)
- [x] Repo scaffold + CI basics
- [x] data_contract.md (Schemas für Bronze/Silver/Gold + BigQuery RAW/CORE/MARTS)
- [x] compliance.md (ToS Hinweise, Demo vs Private Mode)

### MVP-1 (local ingestion → bronze files)
- [x] Kickbase client: auth + snapshot pulls
- [x] LigaInsider scraper: status/lineup + timestamps
- [x] Local scheduler + rate limit + caching
- [x] Bronze file writer + run metadata
- [x] Kickbase League Discovery (`srvl`) + CLI check script
- [x] LigaInsider Multi-Team-Scrape (18 Vereinsseiten) stabilisiert
- [x] Kickbase Bronze erweitert (Marktwert-Historie, Transfers, Performance-Felder via API, mit Fallbacks)
- [x] Kickbase Full-Player-Pool (Competition-Search + Pagination + Dedup, Market nur als Fallback)
- [x] LigaInsider Bronze erweitert (Lineup-Flag, Konkurrenzliste, Change-Tracking `last_changed_at`)
- [x] Selektive private Ingestion-Sources (`--sources kickbase,ligainsider,odds`), damit Teil-Updates ohne kompletten Kickbase-Refresh moeglich sind.
- [x] Wettquoten als dritte Bronze-Quelle (`odds_match_snapshot`) eingebunden (1. Bundesliga, H2H + Totals fuer die naechsten 9 Spiele).
- [ ] Kickbase-Ingestion weiter aufsplitten in fachliche Refresh-Modi (z.B. `kickbase_marketvalue_daily`, `kickbase_matchday_stats`, `kickbase_lineup_status_intraday`), damit nur noetige Endpunkte je Run geladen werden.

### MVP-2 (Databricks bronze/silver/gold jobs)
- [x] Bronze ingest job: load raw files to Delta (lokales Job-Skeleton + Lakehouse Bronze Snapshot Layout)
- [x] Silver sync job: identity + canonical tables (lokales Job-Skeleton)
- [x] Gold job: feature tables + points_components skeleton (lokales Job-Skeleton)
- [x] Databricks Workspace Jobs (kickbase_bronze/silver/gold) per CLI angelegt und auf Repo-Notebooks verdrahtet

### MVP-3 (local ML baseline + backtesting)
- [x] StartProbability baseline (regelbasiertes Baseline-Modell im Gold-Job)
- [x] ExpectedPointsNextMatchday baseline + breakdown table
- [x] Risk via simple Monte Carlo
- [x] Backtesting harness (matchday aligned)

### MVP-4 (BigQuery + Power BI)
- [x] RAW loader (JSONL Export + bq CLI Upload Script)
- [x] CORE transformations (BigQuery SQL Views + Apply Script)
- [x] MARTS views (lokaler Prototype-Builder + BigQuery SQL Views)
- [x] Power BI Desktop Asset-Pack (M/DAX/TMDL Templates + Export Script)
- [ ] Power BI dashboards v0 (leaderboard + breakdown + MW + risk)

### V0.9-Plus (PostgreSQL History Store lokal)
- [x] Docker-Setup fuer lokalen Postgres-Container + optional pgAdmin angelegt.
- [x] Flyway-Migration fuer History-Schema (`dim_player`, `dim_event_type`, `dim_team`, `dim_match`, `fact_market_value_daily`, `fact_player_match`, `fact_player_event`, `etl_state`) angelegt.
- [x] Python-ETL fuer idempotente/inkrementelle Writes in Postgres aufgebaut (Databricks-Driver oder CSV-Fallback).
- [x] Smoke-Test auf Einzelspieler (z.B. Orban) gegen echte API-Daten durchgefuehrt (Marktwert + Performance + Event-Breakdown in Postgres validiert).
- [x] Eventtype-Mapping korrigiert: `/v4/live/eventtypes` (`i`/`ti`) wird vollstaendig geparst, Event-Namen sind damit in `dim_event_type` verfuegbar.
- [x] Match-Kontext in History erweitert: `fact_player_match` mit `is_home` und `match_result` (`W`/`D`/`L`) fuer direkte Win/Draw/Loss-Analysen.
- [x] Legacy-`season_label='unknown'` in Event-Facts wird beim ETL-Lauf pro Spieler bereinigt, wenn bereits ein gleiches Event fuer eine bekannte Saison existiert.
- [x] Source-unabhaengige Player-ID eingefuehrt: `player_uid` (`name_yyyymmdd`) ist in allen spielerbezogenen Fact-Tabellen erste Spalte/Key.
- [x] Kickbase-Source-ID explizit umbenannt: `player_id` -> `kb_player_id` in `dim_player`.
- [x] Match-ID auf kompaktes, lesbares Format umgestellt: `match_uid = <yy/yy>-MD<spieltag>-<home_code><away_code>` (z.B. `25/26-MD23-RBLBVB`).
- [x] `fact_player_match` verschlankt: kein redundantes `opponent_name`; Kontext laeuft ueber `dim_match`.
- [x] Team-Normalisierung in `dim_team`: `team_code` eingefuehrt und `team_name` auf Anzeigeformat `RBL (RB Leipzig)` standardisiert.
- [x] Spielerbild-Feld in `dim_player` ergaenzt: Bilddaten als `BYTEA` + `image_mime` + `image_sha256`.
- [x] Bild-Persistenz auf lokale Datei-Pfade umgestellt: `dim_player.image_local_path` speichert den lokalen Bildpfad (`data/history/player_images/<player_uid>.jpg`) statt externer URL.
- [x] Raw-only Prinzip fuer History-DB geschaerft: abgeleitete Tabelle `fact_match_event_agg` entfernt (Aggregation erst in Analyse/Silver+).
- [x] Team-Schluessel vereinheitlicht: `team_uid` als sprechender Text-Key (primär Teamkuerzel wie `RBL`, `BVB`) statt numerischer Surrogate-Key.
- [x] Legacy-Teamcodes bereinigt: historische `Txx`-UIDs in `dim_team`/`dim_match`/Facts auf stabile Teamkuerzel migriert; `dim_match.match_uid` fuer Bestandsdaten neu aufgebaut.
- [x] Retention fuer History-RAW umgesetzt: nur aktuelle Saison + zwei vorherige Saisons in `dim_match`, `fact_player_match`, `fact_player_event`; Marktwerte analog auf denselben Zeitraum begrenzt.
- [x] Dim-Spalten geschärft: `dim_player` mit `player_birthdate`/`player_position`/`team_uid`/`ligainsider_profile_url`/`image_bytes`; `dim_team` mit `ligainsider_team_url`.
- [x] Identity-Merge eingefuehrt: wenn sich `player_uid` verbessert (z.B. von `...00000000` auf `...YYYYMMDD`), werden bestehende Facts/Bridges konsistent auf den Ziel-Key uebernommen.
- [x] Identity-Merge gehaertet: Unique-Konflikte auf `kb_player_id` abgefangen und `etl_state`-Keys beim UID-Merge auf den Ziel-Key umgehangen.
- [x] Inkrementeller Lasttest abgeschlossen: 5-Spieler-Load erfolgreich gegen Postgres (`market_value`, `fact_player_match`, `fact_player_event`, Team/Match-Dims).
- [x] LigaInsider-Enrichment gehaertet: Fallback-Matching ueber exakten Namen, Nachnamen und Slug; Birthdates/LI-IDs fuer 5er-Testload nachgezogen und UID-Transition (`...00000000` -> `...YYYYMMDD`) erfolgreich validiert.
- [x] 50-Spieler-Batchload erfolgreich gefahren (inkl. Images, Marktwerte, Match/Events) und Laufzeit/API-Rate validiert.
- [x] Competition-Scope-Cleanup aktiviert: `dim_team` auf Bundesliga-Teams begrenzt und nicht-Competition-Matches/Facts aus `dim_match`/`fact_player_match` entfernt.
- [x] Backfill-Skript fuer fehlende `dim_player`-Enrichment-Felder gebaut inkl. JSONL-Issue-Report fuer manuelle Nacharbeit.
- [x] LigaInsider-Snapshot erweitert: pro Team werden jetzt Aufstellungsseite **und** `.../kader/` geparst (vollere Kaderabdeckung inkl. LI-ID, Birthdate, Bild-URL).
- [x] Player-Enrichment-Backfill nach LI-Kader-Erweiterung erneut gelaufen; offene Problemfaelle stark reduziert (Issue-Report bleibt als manueller Fallback aktiv).
- [ ] LigaInsider-Fallback fuer Restfaelle weiter haerten (Suche/Profil-Fallback fuer Einzelfaelle ohne Kader-Match, aktuell z.B. 1 offener Report-Fall).
- [x] Full-Load-Strategie umgesetzt: kontrollierte Batches (5 -> 50 -> Rest in 50ern) inkl. Resume-Offset und API-Rate-Monitoring gefahren (`337/337` Driver verarbeitet).
- [x] Event-Dedupe gehaertet: `fact_player_event.event_hash` nutzt stabile Source-Event-ID (falls vorhanden) statt positionsabhaengigem Index.
- [x] Post-Load-Dedupe auf bestehende Eventdaten angewendet (Duplikate mit gleicher Source-Event-ID bereinigt).
- [ ] Konsistenzcheck als Pflicht-Gate im Batch-Runner verankern (derzeit als separates QA-Skript).
- [ ] Event-Parser fuer Sonderfaelle haerten, bei denen `fact_player_event` trotz vorhandenem `points_total` nur 0-/Meta-Events liefert.
- [ ] HTML-Parser-Haertung fuer LigaInsider: entscheiden, ob Regex-Parser auf `BeautifulSoup` migriert werden soll (Wartbarkeit/Robustheit vs. Runtime).
- [x] Data-Serving-Entscheid finalisiert: BigQuery als Reporting-Warehouse fuer Power BI mit Inputs aus Databricks Gold + ML + selektiver Postgres-History.
- [x] History-Replikation umgesetzt: selektiver Postgres/Bronze-Export nach BigQuery RAW (`hist_*`) statt 1:1 Dump der gesamten History.
- [x] History CORE/MARTS Views fuer Reporting angelegt (`v_hist_*`, `mart_hist_player_profile`, `mart_hist_player_marketvalue_curve`, `mart_hist_player_match_breakdown`, `mart_hist_player_comparison`, `mart_hist_team_outlook`).
- [x] Binary-Policy festgezogen: Spielerbilder (`BYTEA`) bleiben lokal; nach BigQuery nur Bildmetadaten + Pfad/Hash.

### V1.0 (Data Quality + Modeling Upgrade)
- [x] Kickbase Feldmapping validieren (`smc` vs `ismc`) und `average_minutes` korrekt auf Einsaetzen statt Starts berechnen (inkl. Guard gegen Division durch 0).
- [x] Kickbase `team_id` Mapping in Team-Dimension ueberfuehrt (`kickbase_team_id` -> canonical `team_uid` + Anzeige-Name); historische Legacy-IDs sind nun auf stabile Teamkuerzel normalisiert.
- [ ] Kickbase-Ingestion-Frequenzen entkoppeln: eigener Mode fuer Marktwert (taeglich 22:04), Performance/Stats (Spieltage) und Status/Lineup (mehrfach taeglich), um API-Calls zu minimieren.
- [ ] Marktwert-Historie in Bronze vervollstaendigen: 10-Tage-Tuples pro Spieler, `market_value_high_365d`, `market_value_low_365d` aus echter Historie statt Current-Value-Fallback.
- [ ] Fallback-Strategie fuer Marktwert-Historie implementieren, falls kein dedizierter API-Endpunkt verfuegbar ist (Historisierung aus taeglichen Snapshots).
- [x] LigaInsider Konkurrenz-Extraktion auf UI-Logik umstellen: Positions-Kandidaten ueber den gruennen Pfeil/Carousel pro Spieler erfassen.
- [x] LigaInsider Felder `competition_player_count` und `competition_player_names` gegen die echte Positionskonkurrenz validieren (Regression-Tests + Sample-basierte QA).
- [x] Silver v0.9 umgesetzt: `silver.player_snapshot` als Joined Base (Kickbase + LigaInsider, eine Zeile pro Spieler/Snapshot) mit logisch gruppierten Feature-Spalten fuer Punkte, Aufstellchance und Marktwert.
- [x] Silver v0.9 umgesetzt: `silver.team_matchup_snapshot` (eine Zeile pro Team/naechstes Matchup) inkl. Odds-Features und wahrscheinlicher Aufstellung.
- [ ] Stabile Player-Identity in Silver weiter haerten: UID-Regeln mit History-Store synchron halten (inkl. spaeterem persistenten Alias-Mapping bei Konflikten).
- [ ] Silver-Star-Schema fuer spaetere Serving-Layer vorbereiten: Source-IDs (`kickbase_player_id`, `ligainsider_player_slug/id`) als Aliase dokumentieren.
- [ ] Join-Key-Strategie Odds -> Teams/Fixtures in Silver/Gold weiter haerten (Teamnamen-Normalisierung, Heim/Auswaerts-Mapping, QA-Checks).
- [ ] Transfer-Intelligence vorbereiten: globale/indikative League-Transferstroeme pruefen (falls API-seitig verfuegbar), um Marktwertdynamik im Modell zu erklaeren.
- [ ] Start model calibration (logistic/GBM).
- [ ] MW forecast model (delta_7d).
- [ ] Opponent/team strength features.
- [ ] Model registry + experiment tracking.

### Next Steps (2026-02-23)
- [x] Bronze live validiert: alle drei Quellen (`kickbase`, `ligainsider`, `odds`) liefern Daten.
- [x] Bronze Viewer korrigiert: Latest-Timestamp je Tabelle statt nur gemeinsamer Timestamp.
- [x] CI stabilisiert (`bash` Aufruf fuer Lint/Test, kein Execute-Bit-Fehler mehr) und Packaging-Dependencies fuer History-ETL nachgezogen.
- [x] Bronze vereinheitlichen: altes `kickbase_match_stats` final aus lokalen Artefakten entfernt.
- [x] History-Schema/ETL auf Teamkuerzel-UIDs umgestellt (`dim_team.team_uid`, `bridge_player_team.team_uid`, `dim_match.home/away_team_uid`).
- [x] 50-Spieler-Load als naechster Ingestion-Step gefahren und Laufzeit/API-Rate validiert.
- [x] Silver v0.9 umgesetzt: `silver.player_snapshot` als sauberer Join-Layer implementiert.
- [x] Silver v0.9 umgesetzt: `silver.team_matchup_snapshot` mit Odds-Features und wahrscheinlicher Aufstellung.
- [x] Gold v1.0 spezifiziert: klare Feature-Gruppen fuer Startet, Minuten, Rohpunkte, On-Top-Punkte und Marktwert dokumentiert (`docs/ml_workflow.md`).
- [x] ML-Training-Runner gebaut (`scripts/ml/train_hierarchical_models.py`): sklearn vs PyTorch Vergleich je Fold inkl. Artefakt-Export.
- [x] Historical-CV auf Postgres-RAW + Silver-Features als lokaler Pipeline-Step integriert (walk-forward Splits + Fold-Metriken).
- [x] End-to-End Self-Run (ohne manuelle Python-Schritte) im Container gegen lokale Postgres-DB erfolgreich validiert; ML-Artefakte werden in `data/ml_runs/<run_ts>/` erzeugt.
- [x] Champion-Selektion im ML-Run verankert (`live_predictions_champion.csv` + `champion_selection.json`).
- [x] BigQuery-RAW Export/Load fuer ML-Artefakte implementiert (`ml_live_predictions`, `ml_cv_fold_metrics`, `ml_champion_selection`, `ml_run_summary`).
- [x] BigQuery ML CORE/MARTS Views implementiert (`v_ml_*`, `mart_ml_player_predictions`, `mart_ml_model_monitoring`) inkl. Apply-Step in der ML-Pipeline.
- [x] Historical-CV als regelmaessigen Scheduler-Job verankert (lokaler ML-Pipeline-Scheduler inkl. optionalem BigQuery-Upload).
