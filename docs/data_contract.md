# Data Contract (MVP)

## Bronze files

Each ingestion run writes timestamped NDJSON files:

- `kickbase_player_snapshot_YYYY-MM-DDTHHMMSSZ.ndjson`
- `ligainsider_status_snapshot_YYYY-MM-DDTHHMMSSZ.ndjson`
- `odds_match_snapshot_YYYY-MM-DDTHHMMSSZ.ndjson` (optional, wenn `--sources` `odds` enthaelt)

Required audit columns in each row:

- `ingested_at` (ISO-8601 UTC string)
- `source` (`kickbase` or `ligainsider`)
- `source_version` (string tag)
- `run_id` (UUID)

## Ingestion telemetry

`ingestion_runs.ndjson` appends one row per run with:

- `run_id`
- `mode`
- `status`
- `started_at`
- `finished_at`
- `rows_written`
- `files_written`
