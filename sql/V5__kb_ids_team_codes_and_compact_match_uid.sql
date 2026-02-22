-- ------------------------------------
-- V5__kb_ids_team_codes_and_compact_match_uid.sql
--
-- Vereinheitlicht Kickbase-Feldnamen auf kb_player_id,
-- erweitert dim_players um team_code + player_image_url
-- und stellt match_uid auf kompaktes Teamcode-Format um.
-- ------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'dim_players' AND column_name = 'player_id'
    ) THEN
        ALTER TABLE dim_players RENAME COLUMN player_id TO kb_player_id;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'fact_market_value' AND column_name = 'player_id'
    ) THEN
        ALTER TABLE fact_market_value RENAME COLUMN player_id TO kb_player_id;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'fact_match_performance' AND column_name = 'player_id'
    ) THEN
        ALTER TABLE fact_match_performance RENAME COLUMN player_id TO kb_player_id;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'fact_match_events' AND column_name = 'player_id'
    ) THEN
        ALTER TABLE fact_match_events RENAME COLUMN player_id TO kb_player_id;
    END IF;
END
$$;

ALTER TABLE dim_players
    ADD COLUMN IF NOT EXISTS team_code TEXT NULL;

ALTER TABLE dim_players
    ADD COLUMN IF NOT EXISTS player_image_url TEXT NULL;

UPDATE dim_players
SET team_code = UPPER(team_code)
WHERE team_code IS NOT NULL;

UPDATE dim_players
SET team_code = CASE
    WHEN team_name ~ '^[A-Z0-9]{2,5}\s+\(' THEN split_part(team_name, ' ', 1)
    WHEN lower(team_name) LIKE '%bayern%' THEN 'FCB'
    WHEN lower(team_name) LIKE '%dortmund%' THEN 'BVB'
    WHEN lower(team_name) LIKE '%leipzig%' THEN 'RBL'
    WHEN lower(team_name) LIKE '%leverkusen%' THEN 'B04'
    WHEN lower(team_name) LIKE '%wolfsburg%' THEN 'WOB'
    WHEN lower(team_name) LIKE '%stuttgart%' THEN 'VFB'
    WHEN lower(team_name) LIKE '%freiburg%' THEN 'SCF'
    WHEN lower(team_name) LIKE '%frankfurt%' THEN 'SGE'
    WHEN lower(team_name) LIKE '%bremen%' THEN 'SVW'
    WHEN lower(team_name) LIKE '%gladbach%' THEN 'BMG'
    WHEN lower(team_name) LIKE '%koeln%' OR lower(team_name) LIKE '%k%C3%B6ln%' THEN 'KOE'
    WHEN lower(team_name) LIKE '%mainz%' THEN 'M05'
    WHEN lower(team_name) LIKE '%augsburg%' THEN 'FCA'
    WHEN lower(team_name) LIKE '%union%' THEN 'FCU'
    WHEN lower(team_name) LIKE '%st. pauli%' OR lower(team_name) LIKE '%st pauli%' THEN 'STP'
    WHEN lower(team_name) LIKE '%heidenheim%' THEN 'FCH'
    WHEN lower(team_name) LIKE '%hoffenheim%' THEN 'TSG'
    WHEN lower(team_name) LIKE '%hamburg%' OR lower(team_name) LIKE '%hsv%' THEN 'HSV'
    ELSE team_code
END
WHERE team_code IS NULL;

UPDATE dim_players
SET team_name = CASE
    WHEN team_code IS NULL THEN team_name
    WHEN team_name IS NULL OR btrim(team_name) = '' THEN team_code
    WHEN team_name ~ '^[A-Z0-9]{2,5}\s+\(' THEN team_name
    ELSE team_code || ' (' || team_name || ')'
END;

ALTER TABLE fact_match_performance
    DROP COLUMN IF EXISTS match_ts;

ALTER TABLE fact_match_performance
    DROP COLUMN IF EXISTS opponent_name;

WITH team_codes AS (
    SELECT team_id, max(team_code) AS team_code
    FROM dim_players
    WHERE team_id IS NOT NULL
      AND team_code IS NOT NULL
    GROUP BY team_id
)
UPDATE fact_match_performance p
SET match_uid = (
    CASE
        WHEN p.season_label ~ '^\s*\d{4}\s*/\s*\d{4}\s*$'
            THEN right(split_part(p.season_label, '/', 1), 2) || '/' || right(split_part(p.season_label, '/', 2), 2)
        ELSE COALESCE(NULLIF(trim(p.season_label), ''), 'unknown')
    END
    || '-MD'
    || lpad(p.matchday::TEXT, 2, '0')
    || '-'
    || (
        CASE
            WHEN
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.team_id),
                    'T' || COALESCE(p.team_id, -1)::TEXT
                )
                <=
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.opponent_team_id),
                    'T' || COALESCE(p.opponent_team_id, -1)::TEXT
                )
            THEN
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.team_id),
                    'T' || COALESCE(p.team_id, -1)::TEXT
                )
                ||
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.opponent_team_id),
                    'T' || COALESCE(p.opponent_team_id, -1)::TEXT
                )
            ELSE
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.opponent_team_id),
                    'T' || COALESCE(p.opponent_team_id, -1)::TEXT
                )
                ||
                COALESCE(
                    (SELECT tc.team_code FROM team_codes tc WHERE tc.team_id = p.team_id),
                    'T' || COALESCE(p.team_id, -1)::TEXT
                )
        END
    )
)
WHERE TRUE;

UPDATE fact_match_performance
SET match_uid = (
    CASE
        WHEN season_label ~ '^\s*\d{4}\s*/\s*\d{4}\s*$'
            THEN right(split_part(season_label, '/', 1), 2) || '/' || right(split_part(season_label, '/', 2), 2)
        ELSE COALESCE(NULLIF(trim(season_label), ''), 'unknown')
    END
    || '-MD'
    || lpad(matchday::TEXT, 2, '0')
    || '-'
    || 'TNA'
)
WHERE match_uid IS NULL OR btrim(match_uid) = '';

UPDATE fact_match_events e
SET match_uid = p.match_uid
FROM fact_match_performance p
WHERE p.player_uid = e.player_uid
  AND p.competition_id = e.competition_id
  AND p.season_label = e.season_label
  AND p.matchday = e.matchday;

DROP INDEX IF EXISTS idx_dim_players_player_id;
DROP INDEX IF EXISTS idx_fact_market_value_player_id_date;

CREATE INDEX IF NOT EXISTS idx_dim_players_kb_player_id
    ON dim_players (kb_player_id);

CREATE INDEX IF NOT EXISTS idx_fact_market_value_kb_player_id_date
    ON fact_market_value (kb_player_id, mv_date DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_performance_kb_player_id
    ON fact_match_performance (kb_player_id, competition_id, season_label, matchday DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_kb_player_id
    ON fact_match_events (kb_player_id, competition_id, season_label, matchday DESC);
