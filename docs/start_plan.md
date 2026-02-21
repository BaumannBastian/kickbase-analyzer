# Start Plan (Fast Track)

## Ziel
Schneller Projektstart mit sofort lauffaehigen Ergebnissen, aber mit Test- und Contract-Disziplin.

## Arbeitsmodus
- Kurze Iterationen (1-2 Tage pro Inkrement)
- Jedes Inkrement liefert lauffaehigen Code + Tests
- Demo und Private Mode strikt trennen

## Sprint A (jetzt, bereits gestartet)

### Deliverables
- Repo-Scaffold
- Demo Ingestion Runner
- Bronze NDJSON Outputs inkl. Audit Felder
- Unit Tests
- CI Workflow

### Definition of Done
- `./scripts/run_demo_ingestion.sh` erzeugt Bronze Dateien
- `./scripts/test.sh` ist gruen
- CI laeuft auf Push/PR

## Sprint B (in Umsetzung)

### Deliverables
- [x] Kickbase API Client (private mode)
- [x] Kickbase Auth Check Script
- [x] LigaInsider Web Scraper (private mode)
- [x] Konfigurierbares Rate-Limit + Retry
- [x] Bronze Writer fuer echte API Responses
- [x] Scheduler Job (lokal)

### Definition of Done
- Private ingestion laeuft lokal mit `.env`
- Telemetrie in `ingestion_runs.ndjson` vorhanden
- Fehlerfaelle getestet (Auth fail, timeout)

## Sprint C (in Umsetzung)

### Deliverables
- [x] Databricks Job Skeleton: bronze_ingest, silver_sync, gold_features
- [x] Erste Silver Tables (dim_player, map_player_source)
- [x] Erste Gold Feature Table (feat_player_daily)
- [x] Databricks Workspace Jobs fuer kickbase_bronze/silver/gold erstellt

### Definition of Done
- Job-Pipeline einmal end-to-end auf Demo-Daten erfolgreich
- Schema-Pruefung gegen Contract Dokumentation

## Sprint D (in Umsetzung)

### Deliverables
- [x] ML Baselines (StartProbability, ExpectedPointsNextMatchday)
- [x] Risk-Basisoutputs (p10/p50/p90, stddev, p_dnp)
- [x] Risk Outputs via Monte-Carlo
- [x] BigQuery RAW Loader + CORE/MARTS View-Skripte
- [x] BigQuery CLI Setup Skripte (Install/Auth/Check)
- [x] Backtesting Harness (matchday aligned)
- [x] Power BI REST API Skripte (Workspace/Dataset/Refresh)
- [ ] BigQuery MARTS v0 mit echten Cloud-Runs

### Definition of Done
- Backtesting Report fuer mindestens eine historische Periode
- Leaderboard + Breakdown Mart verfuegbar

## Rollen

### Deine Aufgaben
- Priorisieren, welche Features/Outputs als naechstes gebaut werden
- Fachliche Validierung der Ergebnisse
- Zugangsdaten lokal bereitstellen und sicher halten

### Meine Aufgaben
- Umsetzung der Features
- Testabdeckung aufbauen
- Doku und technische Entscheidungen nachziehen

## Heute naechster Schritt
1) BigQuery Cloud-Run mit echtem Projekt testen
2) Danach Power BI Dashboard v0 auf MARTS finalisieren
