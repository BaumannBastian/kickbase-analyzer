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
LIGAINSIDER_STATUS_URL=<team_url_1>,<team_url_2>,...
```

Optionale Auth-Feldnamen (falls Endpoint andere Keys erwartet):

```bash
KICKBASE_AUTH_EMAIL_FIELD=email
KICKBASE_AUTH_PASSWORD_FIELD=password
```

## 2) Kickbase Auth testen

```bash
./scripts/run_kickbase_auth_check.sh --env-file .env --verify-snapshots
```

Erwartung:
- `status = success`
- `player_rows > 0`
- `match_stats_rows > 0`

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
- `data/bronze/kickbase_match_stats_<timestamp>.ndjson`
- `data/bronze/ligainsider_status_snapshot_<timestamp>.ndjson`
- `data/bronze/ingestion_runs.ndjson`

## 5) Optional: Datei-Fallback fuer LigaInsider
Falls Live-Scrape temporär nicht moeglich ist:

```bash
LIGAINSIDER_STATUS_FILE=/pfad/zur/datei.json
```

Wenn `LIGAINSIDER_STATUS_FILE` gesetzt ist, nutzt der Runner diese Datei anstelle des Live-Scrapes.
