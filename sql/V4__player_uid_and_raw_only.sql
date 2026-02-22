-- ------------------------------------
-- V4__player_uid_and_raw_only.sql
--
-- Stellt das History-Schema auf einen source-unabhaengigen Spieler-Key um
-- und entfernt abgeleitete Event-Aggregate aus der Raw-History-DB.
-- ------------------------------------

CREATE TABLE dim_players_v4 (
    player_uid TEXT PRIMARY KEY,
    player_id BIGINT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    birth_date DATE NULL,
    ligainsider_player_slug TEXT NULL,
    ligainsider_player_id BIGINT NULL,
    team_id BIGINT NULL,
    team_name TEXT NULL,
    position TEXT NULL,
    competition_id BIGINT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

WITH src AS (
    SELECT
        p.player_id,
        p.name,
        p.team_id,
        p.team_name,
        p.position,
        p.competition_id,
        p.updated_at,
        NULL::DATE AS birth_date,
        COALESCE(
            NULLIF(
                trim(both '_' FROM lower(regexp_replace(p.name, '[^A-Za-z0-9]+', '_', 'g'))),
                ''
            ),
            'unknown_player'
        ) AS norm_name
    FROM dim_players p
), ranked AS (
    SELECT
        *,
        (norm_name || '_' || COALESCE(to_char(birth_date, 'YYYYMMDD'), '00000000')) AS base_uid,
        row_number() OVER (
            PARTITION BY (norm_name || '_' || COALESCE(to_char(birth_date, 'YYYYMMDD'), '00000000'))
            ORDER BY player_id
        ) AS rn
    FROM src
)
INSERT INTO dim_players_v4 (
    player_uid,
    player_id,
    name,
    birth_date,
    ligainsider_player_slug,
    ligainsider_player_id,
    team_id,
    team_name,
    position,
    competition_id,
    updated_at
)
SELECT
    CASE
        WHEN rn = 1 THEN base_uid
        ELSE base_uid || '__' || player_id::TEXT
    END AS player_uid,
    player_id,
    name,
    birth_date,
    NULL,
    NULL,
    team_id,
    team_name,
    position,
    competition_id,
    updated_at
FROM ranked;

CREATE TABLE fact_market_value_v4 (
    player_uid TEXT NOT NULL REFERENCES dim_players_v4(player_uid) ON UPDATE CASCADE,
    player_id BIGINT NOT NULL,
    mv_date DATE NOT NULL,
    market_value BIGINT NOT NULL,
    source_dt_days INT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_uid, mv_date)
);

INSERT INTO fact_market_value_v4 (
    player_uid,
    player_id,
    mv_date,
    market_value,
    source_dt_days,
    ingested_at
)
SELECT
    d.player_uid,
    f.player_id,
    f.mv_date,
    f.market_value,
    f.source_dt_days,
    f.ingested_at
FROM fact_market_value f
JOIN dim_players_v4 d
  ON d.player_id = f.player_id;

CREATE TABLE fact_match_performance_v4 (
    player_uid TEXT NOT NULL REFERENCES dim_players_v4(player_uid) ON UPDATE CASCADE,
    player_id BIGINT NOT NULL,
    competition_id BIGINT NOT NULL,
    season_label TEXT NOT NULL,
    matchday INT NOT NULL,
    match_uid TEXT NOT NULL,
    match_id BIGINT NULL,
    match_ts TIMESTAMPTZ NULL,
    team_id BIGINT NULL,
    opponent_team_id BIGINT NULL,
    opponent_name TEXT NULL,
    is_home BOOLEAN NULL,
    score_home INT NULL,
    score_away INT NULL,
    match_result CHAR(1) NULL,
    points_total INT NOT NULL,
    raw_json JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_uid, competition_id, season_label, matchday),
    CONSTRAINT chk_fact_match_performance_v4_match_result
        CHECK (match_result IN ('W', 'D', 'L') OR match_result IS NULL)
);

INSERT INTO fact_match_performance_v4 (
    player_uid,
    player_id,
    competition_id,
    season_label,
    matchday,
    match_uid,
    match_id,
    match_ts,
    team_id,
    opponent_team_id,
    opponent_name,
    is_home,
    score_home,
    score_away,
    match_result,
    points_total,
    raw_json,
    ingested_at
)
SELECT
    d.player_uid,
    f.player_id,
    f.competition_id,
    f.season_label,
    f.matchday,
    CONCAT_WS(
        '|',
        f.competition_id::TEXT,
        COALESCE(f.season_label, 'unknown'),
        f.matchday::TEXT,
        COALESCE(to_char(f.match_ts, 'YYYY-MM-DD"T"HH24:MI:SSOF'), 'na'),
        COALESCE(LEAST(f.team_id, f.opponent_team_id)::TEXT, 'na'),
        COALESCE(GREATEST(f.team_id, f.opponent_team_id)::TEXT, 'na')
    ) AS match_uid,
    f.match_id,
    f.match_ts,
    f.team_id,
    f.opponent_team_id,
    f.opponent_name,
    f.is_home,
    f.score_home,
    f.score_away,
    f.match_result,
    f.points_total,
    f.raw_json,
    f.ingested_at
FROM fact_match_performance f
JOIN dim_players_v4 d
  ON d.player_id = f.player_id;

CREATE TABLE fact_match_events_v4 (
    player_uid TEXT NOT NULL REFERENCES dim_players_v4(player_uid) ON UPDATE CASCADE,
    player_id BIGINT NOT NULL,
    competition_id BIGINT NOT NULL,
    season_label TEXT NOT NULL,
    matchday INT NOT NULL,
    match_uid TEXT NULL,
    event_type_id INT NOT NULL REFERENCES dim_event_types(event_type_id),
    event_name TEXT NULL,
    points INT NOT NULL,
    mt INT NULL,
    att TEXT NULL,
    event_dedup_key TEXT NOT NULL,
    raw_event JSONB NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uniq_fact_match_events_v4_dedup UNIQUE (event_dedup_key)
);

INSERT INTO fact_match_events_v4 (
    player_uid,
    player_id,
    competition_id,
    season_label,
    matchday,
    match_uid,
    event_type_id,
    event_name,
    points,
    mt,
    att,
    event_dedup_key,
    raw_event,
    ingested_at
)
SELECT
    d.player_uid,
    e.player_id,
    e.competition_id,
    e.season_label,
    e.matchday,
    p.match_uid,
    e.event_type_id,
    COALESCE(t.name, 'event_' || e.event_type_id::TEXT) AS event_name,
    e.points,
    e.mt,
    e.att,
    e.event_dedup_key,
    e.raw_event,
    e.ingested_at
FROM fact_match_events e
JOIN dim_players_v4 d
  ON d.player_id = e.player_id
LEFT JOIN fact_match_performance_v4 p
  ON p.player_uid = d.player_uid
 AND p.competition_id = e.competition_id
 AND p.season_label = e.season_label
 AND p.matchday = e.matchday
LEFT JOIN dim_event_types t
  ON t.event_type_id = e.event_type_id;

DROP TABLE IF EXISTS fact_match_event_agg;
DROP TABLE fact_match_events;
DROP TABLE fact_match_performance;
DROP TABLE fact_market_value;
DROP TABLE dim_players;

ALTER TABLE dim_players_v4 RENAME TO dim_players;
ALTER TABLE fact_market_value_v4 RENAME TO fact_market_value;
ALTER TABLE fact_match_performance_v4 RENAME TO fact_match_performance;
ALTER TABLE fact_match_events_v4 RENAME TO fact_match_events;

CREATE INDEX idx_dim_players_player_id
    ON dim_players (player_id);

CREATE INDEX idx_fact_market_value_player_uid_date
    ON fact_market_value (player_uid, mv_date DESC);

CREATE INDEX idx_fact_market_value_player_id_date
    ON fact_market_value (player_id, mv_date DESC);

CREATE INDEX idx_fact_match_performance_player_uid
    ON fact_match_performance (player_uid, competition_id, season_label, matchday DESC);

CREATE INDEX idx_fact_match_performance_match_uid
    ON fact_match_performance (match_uid);

CREATE INDEX idx_fact_match_events_player_uid
    ON fact_match_events (player_uid, competition_id, season_label, matchday DESC);

CREATE INDEX idx_fact_match_events_event_type
    ON fact_match_events (event_type_id);
