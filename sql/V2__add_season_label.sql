-- ------------------------------------
-- V2__add_season_label.sql
--
-- Erweitert Performance/Event-Facts um season_label,
-- damit mehrere Saisons ohne Matchday-Kollisionen gespeichert werden.
-- ------------------------------------

ALTER TABLE fact_match_performance
    ADD COLUMN IF NOT EXISTS season_label TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE fact_match_events
    ADD COLUMN IF NOT EXISTS season_label TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE fact_match_event_agg
    ADD COLUMN IF NOT EXISTS season_label TEXT NOT NULL DEFAULT 'unknown';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_match_performance_pkey'
    ) THEN
        ALTER TABLE fact_match_performance DROP CONSTRAINT fact_match_performance_pkey;
    END IF;
END $$;

ALTER TABLE fact_match_performance
    ADD CONSTRAINT fact_match_performance_pkey
    PRIMARY KEY (player_id, competition_id, season_label, matchday);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_match_event_agg_pkey'
    ) THEN
        ALTER TABLE fact_match_event_agg DROP CONSTRAINT fact_match_event_agg_pkey;
    END IF;
END $$;

ALTER TABLE fact_match_event_agg
    ADD CONSTRAINT fact_match_event_agg_pkey
    PRIMARY KEY (player_id, competition_id, season_label, matchday, event_type_id);

CREATE INDEX IF NOT EXISTS idx_fact_match_performance_player_season
    ON fact_match_performance (player_id, competition_id, season_label, matchday DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_events_player_season
    ON fact_match_events (player_id, competition_id, season_label, matchday DESC);

CREATE INDEX IF NOT EXISTS idx_fact_match_event_agg_player_season
    ON fact_match_event_agg (player_id, competition_id, season_label, matchday DESC);
