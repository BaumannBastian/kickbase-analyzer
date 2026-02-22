-- ------------------------------------
-- V1__init.sql
--
-- Initiales Schema fuer Kickbase-History in PostgreSQL.
-- ------------------------------------

CREATE TABLE IF NOT EXISTS dim_players (
    player_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    team_id BIGINT NULL,
    team_name TEXT NULL,
    position TEXT NULL,
    competition_id BIGINT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_event_types (
    event_type_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    template TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_market_value (
    player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
    mv_date DATE NOT NULL,
    market_value BIGINT NOT NULL,
    source_dt_days INT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, mv_date)
);

CREATE TABLE IF NOT EXISTS fact_match_performance (
    player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
    competition_id BIGINT NOT NULL,
    matchday INT NOT NULL,
    match_id BIGINT NULL,
    match_ts TIMESTAMPTZ NULL,
    team_id BIGINT NULL,
    opponent_team_id BIGINT NULL,
    opponent_name TEXT NULL,
    score_home INT NULL,
    score_away INT NULL,
    points_total INT NOT NULL,
    raw_json JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, competition_id, matchday)
);

CREATE TABLE IF NOT EXISTS fact_match_events (
    player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
    competition_id BIGINT NOT NULL,
    matchday INT NOT NULL,
    event_type_id INT NOT NULL REFERENCES dim_event_types(event_type_id),
    points INT NOT NULL,
    mt INT NULL,
    att TEXT NULL,
    event_dedup_key TEXT NOT NULL,
    raw_event JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uniq_fact_match_events_dedup UNIQUE (event_dedup_key)
);

CREATE TABLE IF NOT EXISTS fact_match_event_agg (
    player_id BIGINT NOT NULL REFERENCES dim_players(player_id),
    competition_id BIGINT NOT NULL,
    matchday INT NOT NULL,
    event_type_id INT NOT NULL REFERENCES dim_event_types(event_type_id),
    points_sum INT NOT NULL,
    event_count INT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, competition_id, matchday, event_type_id)
);

CREATE TABLE IF NOT EXISTS etl_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_market_value_player_date
    ON fact_market_value (player_id, mv_date DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_performance_player
    ON fact_match_performance (player_id, competition_id, matchday DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_player
    ON fact_match_events (player_id, competition_id, matchday DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_event_type
    ON fact_match_events (event_type_id);

CREATE INDEX IF NOT EXISTS idx_fact_match_event_agg_player
    ON fact_match_event_agg (player_id, competition_id, matchday DESC);
