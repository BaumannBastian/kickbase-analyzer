# README.md

# Kickbase Analyzer (private portfolio project)

Ein privates Analyse- und Prognose-System für Kickbase, gebaut als **end-to-end Data + ML + BI Pipeline**:

**Local ingestion (Kickbase API + LigaInsider scraping)**
→ **Databricks Lakehouse (Bronze → Silver → Gold, PySpark, Jobs, CLI)**
→ **Local ML/Backtesting**
→ **BigQuery DWH (RAW → CORE → MARTS)**
→ **Power BI Dashboards (private access)**

Dieses Projekt ist strukturell bewusst ähnlich zu meinem Portfolio-Projekt:
- https://github.com/BaumannBastian/house-price-model

---

## Wichtig: Private Nutzung / keine kommerzielle oder öffentliche Nutzung
Dieses Projekt ist als **privates** Tool & Portfolio-Demo gedacht.

- LigaInsider: Nutzung der Webseite ist laut AGB **ausschließlich zu privaten Zwecken** gestattet.  
  Deshalb: **Public Demo enthält keine LigaInsider-Daten** (nur synthetische Demo-Daten). :contentReference[oaicite:6]{index=6}

- Kickbase: Terms erwähnen personal use und verbieten kommerzielle Nutzung/Resale; technische Hilfsmittel, die Vorteil verschaffen, sind untersagt.  
  Deshalb: echte Ingestion nur im **Private Mode** (lokal, mit eigenem Account). :contentReference[oaicite:7]{index=7}

---

## Hinweis: Umsetzung mit Codex
Der Code in diesem Repo wird **kontinuierlich mit Codex (in VS Code)** umgesetzt.  
Die Dokumente (`architecture.md`, `data_contract.md`) dienen als Source of Truth für Codex.

---

## Outputs (pro Spieler)

### Predictions
- StartProbability
- ExpectedPointsNextMatchday (inkl. Explainability Breakdown)
- ExpectedPointsRestOfSeason
- ExpectedMarketValueNextMatchday
- ExpectedMarketValueChange7d

### Risk / Varianz
- P(DNP), StdDev, P10/P50/P90
- Injury uncertainty / rotation risk / competitor hints

### Overall Rating / Value Score
Preis-vs-Output-Rating (risikoadjustiert), um Transferentscheidungen zu vereinfachen.

---

## Dashboard-Ideen (Power BI)

### Expected Points Breakdown (pro Spieler/Spieltag)
Beispiel-Komponenten:
- Base/Rohpunkte EV: `P(play) * E(raw_points_if_play)`
- Scorer EV: `P(scorer) * E(scorer_bonus)`
- Win EV: `P(win) * win_bonus`
- Minutes Bonus EV: `E(minutes_bonus)`
- Cards/Negatives EV: `Σ P(event) * points(event)`

Power BI Visuals:
- Stacked Bar pro Spieler: Komponenten + Tooltip (Probability, points_if_event)
- Risiko-Overlay: p10/p90 / StdDev / P(DNP)

### Market Value Dashboard
- aktueller Marktwert (daily snapshot)
- erwartete MW-Entwicklung nächste x Tage
- expected deltas (1d / 7d)
- optional “going rate” (Marktpreise vs MW)

---

## Architektur & Data Contracts
- docs/architecture.md
- docs/data_contract.md
- docs/compliance.md
- docs/start_plan.md
- docs/external_tools.md
- docs/setup_databricks_bigquery.md
- docs/setup_powerbi_desktop.md
- docs/setup_sources_private.md

---

## Tech Stack

### Local (Ingestion + ML)
- Python 3.11+
- requests/httpx + caching
- BeautifulSoup4
- pydantic
- scikit-learn, optional PyTorch
- pandas/polars für lokale Analysis
- Parquet (pyarrow)

### Databricks
- Databricks Jobs + Databricks CLI (als API)
- PySpark + Delta Lake
- Bronze/Silver/Gold

### BigQuery + BI
- BigQuery RAW/CORE/MARTS (SQL)
- Power BI (Import aus MARTS)

---

## Scheduling / Refresh (Europe/Berlin)
Kickbase Marktwerte (Seasonal) werden täglich **gegen 22:00** aktualisiert. :contentReference[oaicite:8]{index=8}  
Plan:
- Local ingestion Kickbase snapshot: 22:10
- Optional Retry: 22:20

Lineup/Status (Kickbase + LigaInsider) adaptiv:
- normal: alle 3h
- T-24h → T-3h: alle 30min
- T-3h → Kickoff: alle 10min
- After kickoff +2h: 1 final snapshot

---

## Demo Mode vs Private Mode (wichtig für GitHub & Recruiter)

### Demo Mode (public, recruiter-friendly)
- Läuft ohne Kickbase/LigaInsider Zugriff
- Nutzt `demo/data/` (synthetische Daten)
- Erstellt Beispiel-Gold/Pred Tables
- Dient dazu, Pipeline/Modeling/Dashboards nachvollziehbar zu zeigen

### Private Mode (nur lokal)
- Nutzt echte Kickbase Credentials (.env, nicht im Repo)
- Scraping mit Rate-Limit + Cache (privat)
- Lädt echte Daten nach Databricks/BigQuery

---

## Dashboard-Betrieb (ohne Abo/Service-API)
Ziel: Dashboards lokal in Power BI Desktop entwickeln und reproduzierbar halten.
Optionen:
- Power BI Desktop lokal (BigQuery MARTS als Source, manuelle Refreshes)
- Exporte/Screenshots/PDFs fuer Portfolio-Review
- Optional spaeter: separates Sharing-Tool fuer Recruiter

---

## Repo Layout (Kurz)
Siehe docs/architecture.md für die vollständige Struktur.

---

## Setup (aktueller Stand)
1) Optional: virtuelle Umgebung anlegen  
   `python3 -m venv .venv && source .venv/bin/activate`
2) Demo-Ingestion ausführen  
   `./scripts/run_demo_ingestion.sh`
3) Kickbase API Auth prüfen (`.env` erforderlich)  
   `./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots`
4) Kickbase League-ID ermitteln (`srvl`)  
   `./scripts/run_kickbase_league_discovery.sh --env-file .env`
5) LigaInsider Scrape prüfen (`.env` erforderlich)  
   `./scripts/run_ligainsider_scrape_check.sh --env-file .env`
6) Private-Ingestion ausführen (`.env` erforderlich)  
   `./scripts/run_private_ingestion.sh --env-file .env --sources kickbase,ligainsider`
7) Nur LigaInsider aktualisieren (ohne kompletten Kickbase-Refresh)  
   `./scripts/run_private_ingestion.sh --env-file .env --sources ligainsider`
8) Wettquoten (The Odds API) als dritte Bronze-Tabelle laden  
   `./scripts/run_private_ingestion.sh --env-file .env --sources odds`
9) Scheduler ausführen (z.B. alle 30 Minuten)  
   `./scripts/run_scheduler.sh --mode private --sources kickbase,ligainsider --interval-seconds 1800`
10) Databricks-Job-Skeleton lokal ausführen (Bronze → Silver → Gold)  
   `./scripts/run_databricks_jobs_demo.sh`
11) MARTS lokal erzeugen (bewertbarer Output)  
   `./scripts/run_build_marts_local.sh`
12) End-to-End in einem Lauf  
   `./scripts/run_pipeline_demo.sh`
13) Backtesting-Report erzeugen  
   `./scripts/run_backtesting.sh`
14) Tests ausführen  
   `./scripts/test.sh`
15) Lint/Compile-Check  
   `./scripts/lint.sh`
16) Bronze-Outputs prüfen  
   `data/bronze/*.ndjson`
17) Externe Toolchain prüfen  
   `./scripts/check_external_tools.sh`
18) Databricks + BigQuery Setup (Step-by-Step)  
   `docs/setup_databricks_bigquery.md`
19) BigQuery CLI installieren/authentifizieren (WSL)  
   `./scripts/bigquery/install_gcloud_cli_wsl.sh`  
   `./scripts/bigquery/configure_gcloud_auth.sh --project <gcp_project_id>`  
   `./scripts/bigquery/check_bq_setup.sh`
20) Power BI Desktop Setup (lokal)  
   `docs/setup_powerbi_desktop.md`

Hinweis: `private` mode ist implementiert und benoetigt eine korrekte `.env` Konfiguration.

Bewertbarer Output (Demo) liegt nach Pipeline-Run in:
- `data/marts/mart_player_leaderboard_<timestamp>.csv`
- `data/marts/mart_points_breakdown_<timestamp>.csv`
- `data/marts/mart_risk_overview_<timestamp>.csv`
- `data/backtesting/backtest_summary_<timestamp>.csv`
- `data/backtesting/backtest_errors_<timestamp>.csv`

BigQuery RAW Exporte (fuer Upload) liegen in:
- `data/warehouse/raw/feat_player_daily.jsonl`
- `data/warehouse/raw/points_components_matchday.jsonl`
- `data/warehouse/raw/quality_metrics.jsonl`
- `data/warehouse/raw/manifest.json`

BigQuery CLI Runner:
- `./scripts/bigquery/install_gcloud_cli_wsl.sh`
- `./scripts/bigquery/configure_gcloud_auth.sh --project <gcp_project_id>`
- `./scripts/bigquery/check_bq_setup.sh`
- `./scripts/bigquery/run_prepare_raw_exports.sh`
- `./scripts/bigquery/run_load_raw_bq.sh --project <gcp_project_id>`
- `./scripts/bigquery/run_apply_views_bq.sh --project <gcp_project_id>`
- `./scripts/bigquery/run_bigquery_pipeline.sh --project <gcp_project_id>`

Source Connectivity Checks:
- `./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots`
- `./scripts/run_kickbase_league_discovery.sh --env-file .env`
- `./scripts/run_ligainsider_scrape_check.sh --env-file .env`
- `python3 -m local_ingestion.runners.run_ingestion --mode private --sources odds --env-file .env`

Hinweis LigaInsider URL:
- Nicht die Homepage (`https://www.ligainsider.de/`) nutzen
- Stattdessen Team-Aufstellungsseiten (eine oder mehrere, comma-separated in `LIGAINSIDER_STATUS_URL`)

Power BI Desktop Asset Pack:
- `python3 -m scripts.powerbi_desktop.export_desktop_assets --project <gcp_project_id>`
- siehe `docs/setup_powerbi_desktop.md`

Databricks Repo/Job Runner:
- `python3 -m scripts.databricks.sync_repo --repo-path \"/Repos/<user>/kickbase-analyzer\" --branch main`
- `python3 -m scripts.databricks.create_lakehouse_jobs --repo-path \"/Repos/<user>/kickbase-analyzer\" --profile \"<profile>\"`
- `python3 -m scripts.databricks.run_lakehouse_jobs --stage all --job-id-bronze <id> --job-id-silver <id> --job-id-gold <id>`

---

## Fast-Track Startplan (mit Codex)

### Ziel: schnellstes sinnvolles Tempo, ohne Qualitätsverlust
1) Sprint A (jetzt): Repo-Grundlage + Demo-Ingestion + Tests + CI
2) Sprint B: Kickbase Client + Bronze private snapshots + Scheduler
3) Sprint C: Databricks Bronze/Silver/Gold Skeleton + erste Gold Feature Tables
4) Sprint D: ML Baselines + Backtesting + BigQuery MARTS v0

### Aufgabenteilung (klar)
**Du**
- Prioritäten und Produktentscheidungen (welche Metriken zuerst)
- Credentials/Secrets lokal pflegen (`.env`, nie im Git)
- Fachliche Abnahme der Outputs (Plausibilität statt nur Technik)

**Codex**
- Implementierung, Refactoring, Tests, CI/CD
- Data Contracts und Laufzeit-Skripte
- Kleine, schnelle Iterationen mit sofort lauffähigen Zwischenergebnissen

---

## Roadmap (Kurz)
- MVP: Ingestion + Bronze/Silver/Gold + Baseline Predictions + BigQuery + Power BI v0
- V1.0 Fokus:
- Kickbase Data Quality: `smc`/`ismc` Mapping fixen, `average_minutes` korrigieren, `team_id` Mapping dokumentieren.
- Marktwert-Historie robust machen: 10d Tuples + 365d High/Low aus historisierten Daten.
- LigaInsider Konkurrenzlogik neu: Positions-Konkurrenz ueber den gruennen Pfeil/Carousel extrahieren.
- V0.9 Silver: zwei Tabellen bauen (`silver.player_snapshot`, `silver.team_matchup_snapshot`) mit sauberem Join aus Kickbase, LigaInsider und Odds.
- Stabile Player-ID in Silver: eigener fortlaufender `player_uid` mit persistenter Mapping-Historie.
- Wettquoten sind in Bronze aktiv (`odds_match_snapshot`); naechster Schritt ist der robuste Team/Fixture-Join in Silver/Gold.
- V1.0 Modelle: Kalibrierung StartProbability, MW Forecast (`delta_7d`), Opponent/Team-Strength Features, Backtesting-Haertung.
