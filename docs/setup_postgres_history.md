# docs/setup_postgres_history.md

# Setup: Lokale PostgreSQL History DB (Docker + Flyway + ETL)

## Ziel
Dieses Setup historisiert Kickbase-Daten in Postgres:
- Marktwert-Historie
- Match-Performance pro Spieltag
- Event-Breakdown pro Spieltag

## Voraussetzungen
- Docker Desktop laeuft
- `.env` ist vorhanden (Kickbase Credentials gesetzt)
- Python 3.11+

## 1) Dependencies installieren

```powershell
python -m pip install -r requirements.txt
```

## 2) Postgres starten

```powershell
.\scripts\start_db.ps1
```

## 3) Schema migrieren

Option A (lokales Flyway installiert):

```powershell
.\scripts\run_flyway_migrate.ps1
```

Option B (Flyway in Docker):

```powershell
docker run --rm --network kickbaseanalyzer_default `
  -e PGHOST=postgres -e PGPORT=5432 -e PGDATABASE=kickbase_history -e PGUSER=kickbase -e PGPASSWORD=kickbase `
  -v "${PWD}\sql:/flyway/sql" `
  -v "${PWD}\flyway.conf:/flyway/conf/flyway.conf" `
  redgate/flyway:10 -configFiles=/flyway/conf/flyway.conf migrate
```

## 4) Smoke-Test mit 1 Spieler (Orban)

Variante mit CSV (empfohlen fuer kontrollierten Start):

```powershell
python -m src.etl_history --env-file .env --players-csv .\in\orban.csv --max-players 1 --competition-id 1 --timeframe-days 3650 --days-from 1 --days-to 3 --rps 2 --save-raw
```

## 5) Ergebnisse pruefen

```powershell
docker exec -it kickbase-history-postgres psql -U kickbase -d kickbase_history -c "\dt"
docker exec -it kickbase-history-postgres psql -U kickbase -d kickbase_history -c "SELECT player_uid, min(mv_date), max(mv_date), count(*) FROM kickbase_raw.fact_market_value_daily GROUP BY 1;"
docker exec -it kickbase-history-postgres psql -U kickbase -d kickbase_history -c "SELECT player_uid, kb_player_id, player_name, image_mime, image_sha256 FROM kickbase_raw.dim_player ORDER BY updated_at DESC LIMIT 20;"
docker exec -it kickbase-history-postgres psql -U kickbase -d kickbase_history -c "SELECT player_uid, match_uid, points_total, is_home, match_result FROM kickbase_raw.fact_player_match ORDER BY ingested_at DESC LIMIT 20;"
docker exec -it kickbase-history-postgres psql -U kickbase -d kickbase_history -c "SELECT e.player_uid, e.match_uid, e.event_type_id, t.event_name, e.points, e.mt FROM kickbase_raw.fact_player_event e LEFT JOIN kickbase_raw.dim_event_type t ON t.event_type_id = e.event_type_id ORDER BY e.ingested_at DESC, e.mt ASC NULLS LAST LIMIT 25;"
```

## 6) Regelmaessiges Update (inkrementell)

```powershell
python -m src.etl_history --env-file .env --players-csv .\in\orban.csv --max-players 1 --competition-id 1 --timeframe-days 3650 --days-from 1 --rps 2
```

Hinweis:
- Durch PK/UNIQUE + `ON CONFLICT` ist der Lauf idempotent.
- Mit mehr Spielern zuerst schrittweise erhoehen (`--max-players 5`, dann 25, ...).
