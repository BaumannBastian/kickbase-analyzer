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
- Predictions/Components werden als Tabellen exportiert

### Step D — BigQuery Data Warehouse: RAW → CORE → MARTS
- Upload Predictions + ausgewählte Gold Tables in BigQuery RAW
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
- silver.dim_player (player_uid, canonical_name, position, team_uid, …)
- silver.map_player_source (player_uid ↔ kickbase_id ↔ ligainsider_slug)
- silver.fct_player_daily (daily canonical snapshot: market_value, status, …)
- silver.fct_player_match (match-level canonical stats/events)

Key: player_uid als intern stabiler Identifier.

Identity Strategy:
- deterministic match (IDs/slug)
- fallback fuzzy match (name + team + position)
- manueller Override-Layer: silver.player_identity_overrides

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

### CORE
- typisierte Facts/Dims
- standardisierte Keys (player_uid, team_uid, season, matchday)
- Datums-/Zeitnormierung
- Slowly-changing dims optional

### MARTS (Power BI ready)
- mart_player_leaderboard (Value Score ranking, filterbar)
- mart_points_breakdown (Komponenten-Table, perfekt für stacked bars)
- mart_risk_overview (p10/p90/stddev/p_dnp)
- mart_marketvalue_dashboard (curve + deltas + current)

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

### V1.0 (Data Quality + Modeling Upgrade)
- [x] Kickbase Feldmapping validieren (`smc` vs `ismc`) und `average_minutes` korrekt auf Einsaetzen statt Starts berechnen (inkl. Guard gegen Division durch 0).
- [ ] Kickbase `team_id` Mapping dokumentieren und in eine Team-Dimension ueberfuehren (`kickbase_team_id` -> club_name, season, canonical_team_uid), damit IDs >18 nachvollziehbar sind.
- [ ] Marktwert-Historie in Bronze vervollstaendigen: 10-Tage-Tuples pro Spieler, `market_value_high_365d`, `market_value_low_365d` aus echter Historie statt Current-Value-Fallback.
- [ ] Fallback-Strategie fuer Marktwert-Historie implementieren, falls kein dedizierter API-Endpunkt verfuegbar ist (Historisierung aus taeglichen Snapshots).
- [x] LigaInsider Konkurrenz-Extraktion auf UI-Logik umstellen: Positions-Kandidaten ueber den gruennen Pfeil/Carousel pro Spieler erfassen.
- [x] LigaInsider Felder `competition_player_count` und `competition_player_names` gegen die echte Positionskonkurrenz validieren (Regression-Tests + Sample-basierte QA).
- [ ] Silver v0.9 umsetzen: `silver.player_snapshot` als Joined Base (Kickbase + LigaInsider, eine Zeile pro Spieler/Snapshot) mit logisch gruppierten Feature-Spalten fuer Punkte, Aufstellchance und Marktwert.
- [ ] Silver v0.9 umsetzen: `silver.team_matchup_snapshot` (eine Zeile pro Team/naechstes Matchup) inkl. Wettquoten-basiertem wahrscheinlichsten Ergebnis und einfacher Formkurve.
- [ ] Stabile Player-Identity in Silver finalisieren: eigener interner `player_uid` (fortlaufend), Name/Slug-Matching, persistentes Mapping, inaktive Zuordnungen historisieren.
- [ ] Silver-Star-Schema vorbereiten: `dim_player` mit internem Surrogate-Key (`player_uid`) als zentrale PK, Source-IDs (`kickbase_player_id`, `ligainsider_player_slug/id`) nur als Mapping-Aliase halten.
- [ ] Join-Key-Strategie Odds -> Teams/Fixtures in Silver/Gold haerten (Teamnamen-Normalisierung, Heim/Auswaerts-Mapping, QA-Checks).
- [ ] Transfer-Intelligence vorbereiten: globale/indikative League-Transferstroeme pruefen (falls API-seitig verfuegbar), um Marktwertdynamik im Modell zu erklaeren.
- [ ] Start model calibration (logistic/GBM).
- [ ] MW forecast model (delta_7d).
- [ ] Opponent/team strength features.
- [ ] Model registry + experiment tracking.

### V0.9-Plus (PostgreSQL History Store lokal)
- [x] Docker-Setup fuer lokalen Postgres-Container + optional pgAdmin angelegt.
- [x] Flyway-Migration fuer History-Schema (`dim_players`, `dim_event_types`, `fact_market_value`, `fact_match_performance`, `fact_match_events`, `etl_state`) angelegt.
- [x] Python-ETL fuer idempotente/inkrementelle Writes in Postgres aufgebaut (Databricks-Driver oder CSV-Fallback).
- [x] Smoke-Test auf Einzelspieler (z.B. Orban) gegen echte API-Daten durchgefuehrt (Marktwert + Performance + Event-Breakdown in Postgres validiert).
- [x] Eventtype-Mapping korrigiert: `/v4/live/eventtypes` (`i`/`ti`) wird vollstaendig geparst, Event-Namen sind damit in `dim_event_types` verfuegbar.
- [x] Match-Kontext in History erweitert: `fact_match_performance` mit `is_home` und `match_result` (`W`/`D`/`L`) fuer direkte Win/Draw/Loss-Analysen.
- [x] Legacy-`season_label='unknown'` in Event-Facts wird beim ETL-Lauf pro Spieler bereinigt, wenn bereits ein gleiches Event fuer eine bekannte Saison existiert.
- [x] Source-unabhaengige Player-ID eingefuehrt: `player_uid` (`name_yyyymmdd`) ist in allen spielerbezogenen Fact-Tabellen erste Spalte/Key.
- [x] Raw-only Prinzip fuer History-DB geschaerft: abgeleitete Tabelle `fact_match_event_agg` entfernt (Aggregation erst in Analyse/Silver+).
- [ ] Danach schrittweise auf Full-Roster hochskalieren (Rate-Limit/Retry beibehalten).

### Tomorrow (2026-02-22)
- [x] Bronze live validiert: alle drei Quellen (`kickbase`, `ligainsider`, `odds`) liefern Daten.
- [x] Bronze Viewer korrigiert: Latest-Timestamp je Tabelle statt nur gemeinsamer Timestamp.
- [x] CI stabilisiert (`bash` Aufruf fuer Lint/Test, kein Execute-Bit-Fehler mehr).
- [x] Bronze vereinheitlichen: altes `kickbase_match_stats` final aus lokalen Artefakten entfernt.
- [ ] Silver v0.9 starten: `silver.player_snapshot` als sauberer Join-Layer implementieren.
- [ ] Silver v0.9 starten: `silver.team_matchup_snapshot` mit Odds-Features und Formkurve.
- [ ] Gold v1.0 spezifizieren: klare Feature-Gruppen fuer Startet, Punkte, Marktwert.
