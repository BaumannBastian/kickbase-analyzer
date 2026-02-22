-- ------------------------------------
-- V6__raw_star_schema_with_image_blob.sql
--
-- Neues RAW-Star-Schema in kickbase_raw:
-- - interne player_uid (BIGINT) als zentrale PK
-- - Kickbase-ID separat als Mapping
-- - Spielerbild als BYTEA + MIME + SHA256
-- - keine *_agg Tabellen
-- ------------------------------------

CREATE SCHEMA IF NOT EXISTS kickbase_raw;

CREATE TABLE IF NOT EXISTS kickbase_raw.dim_player (
    player_uid BIGINT PRIMARY KEY,
    kb_player_id BIGINT NULL UNIQUE,
    player_name TEXT NOT NULL,
    position TEXT NULL,
    birthdate DATE NULL,
    image_blob BYTEA NULL,
    image_mime TEXT NULL,
    image_sha256 TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kickbase_raw.dim_season (
    season_uid BIGSERIAL PRIMARY KEY,
    league_key TEXT NOT NULL,
    season_label TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_season UNIQUE (league_key, season_label)
);

CREATE TABLE IF NOT EXISTS kickbase_raw.dim_team (
    team_uid BIGSERIAL PRIMARY KEY,
    league_key TEXT NOT NULL,
    kickbase_team_id BIGINT NULL,
    team_code TEXT NULL,
    team_name TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dim_team_league_team_code UNIQUE (league_key, team_code),
    CONSTRAINT uq_dim_team_league_kickbase_team_id UNIQUE (league_key, kickbase_team_id)
);

CREATE TABLE IF NOT EXISTS kickbase_raw.bridge_player_team (
    player_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_player(player_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    season_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_season(season_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    team_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_team(team_uid) ON UPDATE CASCADE ON DELETE RESTRICT,
    valid_from DATE NULL,
    valid_to DATE NULL,
    source TEXT NOT NULL DEFAULT 'kickbase',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_uid, season_uid, source)
);

CREATE TABLE IF NOT EXISTS kickbase_raw.dim_match (
    match_uid TEXT PRIMARY KEY,
    kickbase_match_id BIGINT NULL UNIQUE,
    league_key TEXT NOT NULL,
    season_uid BIGINT NULL REFERENCES kickbase_raw.dim_season(season_uid) ON UPDATE CASCADE ON DELETE SET NULL,
    season_label TEXT NOT NULL,
    matchday INT NOT NULL,
    home_team_uid BIGINT NULL REFERENCES kickbase_raw.dim_team(team_uid) ON UPDATE CASCADE ON DELETE SET NULL,
    away_team_uid BIGINT NULL REFERENCES kickbase_raw.dim_team(team_uid) ON UPDATE CASCADE ON DELETE SET NULL,
    kickoff_ts TIMESTAMPTZ NULL,
    score_home INT NULL,
    score_away INT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kickbase_raw.dim_event_type (
    event_type_id INT PRIMARY KEY,
    event_name TEXT NOT NULL,
    template TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kickbase_raw.fact_market_value_daily (
    player_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_player(player_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    mv_date DATE NOT NULL,
    market_value BIGINT NOT NULL,
    source_dt_days INT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_uid, mv_date)
);

CREATE TABLE IF NOT EXISTS kickbase_raw.fact_player_match (
    player_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_player(player_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    match_uid TEXT NOT NULL REFERENCES kickbase_raw.dim_match(match_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    points_total INT NOT NULL,
    is_home BOOLEAN NULL,
    match_result CHAR(1) NULL,
    raw_json JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_uid, match_uid),
    CONSTRAINT chk_fact_player_match_result
        CHECK (match_result IN ('W', 'D', 'L') OR match_result IS NULL)
);

CREATE TABLE IF NOT EXISTS kickbase_raw.fact_player_event (
    event_hash TEXT PRIMARY KEY,
    player_uid BIGINT NOT NULL REFERENCES kickbase_raw.dim_player(player_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    match_uid TEXT NOT NULL REFERENCES kickbase_raw.dim_match(match_uid) ON UPDATE CASCADE ON DELETE CASCADE,
    event_type_id INT NOT NULL REFERENCES kickbase_raw.dim_event_type(event_type_id),
    points INT NOT NULL,
    mt INT NULL,
    att TEXT NULL,
    raw_event JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS kickbase_raw.etl_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_player_kb_player_id
    ON kickbase_raw.dim_player (kb_player_id);

CREATE INDEX IF NOT EXISTS idx_dim_player_image_sha256
    ON kickbase_raw.dim_player (image_sha256);

CREATE INDEX IF NOT EXISTS idx_dim_team_league_key
    ON kickbase_raw.dim_team (league_key);

CREATE INDEX IF NOT EXISTS idx_dim_match_lookup
    ON kickbase_raw.dim_match (league_key, season_label, matchday);

CREATE INDEX IF NOT EXISTS idx_fact_market_value_daily_player_date
    ON kickbase_raw.fact_market_value_daily (player_uid, mv_date DESC);

CREATE INDEX IF NOT EXISTS idx_fact_player_match_match_uid
    ON kickbase_raw.fact_player_match (match_uid);

CREATE INDEX IF NOT EXISTS idx_fact_player_event_player
    ON kickbase_raw.fact_player_event (player_uid, match_uid);

CREATE INDEX IF NOT EXISTS idx_fact_player_event_event_type
    ON kickbase_raw.fact_player_event (event_type_id);
