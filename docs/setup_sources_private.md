# Private Source Setup (Kickbase + LigaInsider)

## Ziel
Private Ingestion mit echten Kickbase Daten und LigaInsider Status-Snapshots.

## 1) `.env` vorbereiten
Kopiere `.env.example` nach `.env` und setze mindestens:

```bash
KICKBASE_BASE_URL=<kickbase_api_base_url>
KICKBASE_LEAGUE_ID=<league_id>
KICKBASE_EMAIL=<login_email>
KICKBASE_PASSWORD=<login_password>
KICKBASE_USER_AGENT=okhttp/4.11.0
LIGAINSIDER_STATUS_URL=<team_url_1>,<team_url_2>,...
```

Optionale Auth-Feldnamen (falls Endpoint andere Keys erwartet):

```bash
KICKBASE_AUTH_EMAIL_FIELD=email
KICKBASE_AUTH_PASSWORD_FIELD=password
```

Empfohlene Kickbase-v4 Defaults:

```bash
KICKBASE_BASE_URL=https://api.kickbase.com
KICKBASE_AUTH_PATH=/v4/user/login
KICKBASE_AUTH_EMAIL_FIELD=em
KICKBASE_AUTH_PASSWORD_FIELD=pass
KICKBASE_USER_AGENT=okhttp/4.11.0
KICKBASE_COMPETITION_ID=
KICKBASE_COMPETITION_PLAYERS_SEARCH_PATH=/v4/competitions/{competition_id}/players/search
KICKBASE_COMPETITION_PLAYERS_PAGE_SIZE=100
KICKBASE_COMPETITION_PLAYERS_QUERY=
KICKBASE_PLAYER_SNAPSHOT_PATH=/v4/leagues/{league_id}/market   # Fallback
KICKBASE_MATCH_STATS_PATH=
KICKBASE_PLAYER_DETAILS_PATH=/v4/leagues/{league_id}/players/{player_id}
KICKBASE_PLAYER_MARKET_VALUE_HISTORY_PATH=/v4/players/{player_id}/market-value
KICKBASE_PLAYER_PERFORMANCE_PATH=/v4/players/{player_id}/performance
KICKBASE_PLAYER_TRANSFERS_PATH=/v4/leagues/{league_id}/players/{player_id}/transfers
```

Hinweis:
- Der Ingestion-Runner zieht standardmaessig den **kompletten Liga-Spielerpool** ueber `competitions/{competition_id}/players/search` (mit Pagination).
- `KICKBASE_COMPETITION_ID` wird automatisch aus der Login-Antwort (`srvl[].cpi`) erkannt, falls leer.
- `KICKBASE_PLAYER_SNAPSHOT_PATH` bleibt als technischer Fallback auf den League-Market-Endpoint aktiv.

League-ID automatisch ermitteln:

```bash
./scripts/run_kickbase_league_discovery.sh --env-file .env
```

## 2) Kickbase Auth testen

```bash
./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots
```

Erwartung:
- `status = success`
- `player_rows > 0`
- `player_source = competition_players` (wenn Competition-Endpoint verfuegbar)
- `match_stats_rows` kann `0` sein, wenn `KICKBASE_MATCH_STATS_PATH` leer ist

## 3) LigaInsider Scrape testen

```bash
./scripts/run_ligainsider_scrape_check.sh --env-file .env
```

Erwartung:
- `status = success`
- `row_count > 0`

Wichtig: Die LigaInsider Homepage (`https://www.ligainsider.de/`) enthaelt nicht die benoetigten Team-Aufstellungsdaten im erwarteten Format.
Nutze Team-Aufstellungsseiten wie z. B.:
- `https://www.ligainsider.de/fc-bayern-muenchen/1/`
- `https://www.ligainsider.de/borussia-dortmund/14/`

## 4) Private Ingestion ausfuehren

```bash
./scripts/run_private_ingestion.sh --env-file .env
```

Outputs:
- `data/bronze/kickbase_player_snapshot_<timestamp>.ndjson`
- `data/bronze/ligainsider_status_snapshot_<timestamp>.ndjson`
- `data/bronze/ingestion_runs.ndjson`

## 5) Optional: Datei-Fallback fuer LigaInsider
Falls Live-Scrape temporär nicht moeglich ist:

```bash
LIGAINSIDER_STATUS_FILE=/pfad/zur/datei.json
```

Wenn `LIGAINSIDER_STATUS_FILE` gesetzt ist, nutzt der Runner diese Datei anstelle des Live-Scrapes.
