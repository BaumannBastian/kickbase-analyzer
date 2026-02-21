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

## Hosting für Recruiter (privat)
Ziel: Dashboards “private” halten (nur mit Freigabe/Passwort).
Optionen:
- Power BI Service: Zugriff per Invite/Workspace/App (gezielte Freigabe)
- Oder alternativ: Dashboard-Export + passwortgeschützter Link/Storage (nur Demo)

Wichtig: Öffentliche Nutzung / “für alle” ist nicht Ziel dieses Projekts.

---

## Repo Layout (Kurz)
Siehe docs/architecture.md für die vollständige Struktur.

---

## Setup (aktueller Stand)
1) Optional: virtuelle Umgebung anlegen  
   `python3 -m venv .venv && source .venv/bin/activate`
2) Demo-Ingestion ausführen  
   `./scripts/run_demo_ingestion.sh`
3) Private-Ingestion ausführen (`.env` erforderlich)  
   `./scripts/run_private_ingestion.sh --env-file .env`
4) Scheduler ausführen (z.B. alle 30 Minuten)  
   `./scripts/run_scheduler.sh --mode private --interval-seconds 1800`
5) Databricks-Job-Skeleton lokal ausführen (Bronze → Silver → Gold)  
   `./scripts/run_databricks_jobs_demo.sh`
6) MARTS lokal erzeugen (bewertbarer Output)  
   `./scripts/run_build_marts_local.sh`
7) End-to-End in einem Lauf  
   `./scripts/run_pipeline_demo.sh`
8) Tests ausführen  
   `./scripts/test.sh`
9) Lint/Compile-Check  
   `./scripts/lint.sh`
10) Bronze-Outputs prüfen  
   `data/bronze/*.ndjson`
11) Externe Toolchain prüfen  
   `./scripts/check_external_tools.sh`

Hinweis: `private` mode ist implementiert und benoetigt eine korrekte `.env` Konfiguration.

Bewertbarer Output (Demo) liegt nach Pipeline-Run in:
- `data/marts/mart_player_leaderboard_<timestamp>.csv`
- `data/marts/mart_points_breakdown_<timestamp>.csv`
- `data/marts/mart_risk_overview_<timestamp>.csv`

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
- v1: Kalibrierung StartProbability, Monte-Carlo Risk, MW Forecast delta_7d, Backtesting
