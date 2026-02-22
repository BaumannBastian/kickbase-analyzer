-- ------------------------------------
-- V12__dim_columns_and_season_retention.sql
--
-- 1) Harmonisiert Dim-Spaltennamen/Felder fuer Player/Team
-- 2) Erzwingt Retention-Fenster (aktuelle Saison + 2 vorherige)
--    fuer Match- und Marktwert-Historie
-- ------------------------------------

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'birthdate'
    ) THEN
        ALTER TABLE kickbase_raw.dim_player
            RENAME COLUMN birthdate TO player_birthdate;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'position'
    ) THEN
        ALTER TABLE kickbase_raw.dim_player
            RENAME COLUMN position TO player_position;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'kickbase_raw'
          AND table_name = 'dim_player'
          AND column_name = 'ligainsider_player_name'
    ) THEN
        ALTER TABLE kickbase_raw.dim_player
            RENAME COLUMN ligainsider_player_name TO ligainsider_name;
    END IF;
END $$;

ALTER TABLE kickbase_raw.dim_player
    ADD COLUMN IF NOT EXISTS ligainsider_profile_url TEXT;

ALTER TABLE kickbase_raw.dim_player
    ADD COLUMN IF NOT EXISTS team_uid TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dim_player_team_uid_fkey'
          AND connamespace = 'kickbase_raw'::regnamespace
    ) THEN
        ALTER TABLE kickbase_raw.dim_player
            ADD CONSTRAINT dim_player_team_uid_fkey
            FOREIGN KEY (team_uid)
            REFERENCES kickbase_raw.dim_team(team_uid)
            ON UPDATE CASCADE
            ON DELETE SET NULL;
    END IF;
END $$;

ALTER TABLE kickbase_raw.dim_player
    ADD COLUMN IF NOT EXISTS image_bytes INT GENERATED ALWAYS AS (octet_length(image_blob)) STORED;

ALTER TABLE kickbase_raw.dim_team
    ADD COLUMN IF NOT EXISTS ligainsider_team_url TEXT;

UPDATE kickbase_raw.dim_player
SET ligainsider_profile_url = CASE
    WHEN ligainsider_player_slug IS NULL OR btrim(ligainsider_player_slug) = '' THEN ligainsider_profile_url
    WHEN ligainsider_player_id IS NOT NULL THEN
        'https://www.ligainsider.de/' || ligainsider_player_slug || '_' || ligainsider_player_id::text || '/'
    ELSE
        'https://www.ligainsider.de/' || ligainsider_player_slug || '/'
END
WHERE ligainsider_profile_url IS NULL;

UPDATE kickbase_raw.dim_player AS d
SET
    team_uid = x.team_uid,
    updated_at = now()
FROM (
    SELECT DISTINCT ON (player_uid)
        player_uid,
        team_uid
    FROM kickbase_raw.bridge_player_team
    ORDER BY player_uid, season_uid DESC, updated_at DESC
) AS x
WHERE d.player_uid = x.player_uid
  AND d.team_uid IS DISTINCT FROM x.team_uid;

DO $$
DECLARE
    max_season_uid INT;
    min_keep_season_uid INT;
    cutoff_market_date DATE;
    start_yy INT;
    start_year INT;
BEGIN
    SELECT max(season_uid) INTO max_season_uid
    FROM kickbase_raw.dim_match
    WHERE season_uid IS NOT NULL;

    IF max_season_uid IS NULL THEN
        RETURN;
    END IF;

    -- aktuelle Saison + zwei vorherige Saisons
    min_keep_season_uid := max_season_uid - 202;

    start_yy := min_keep_season_uid / 100;
    start_year := CASE WHEN start_yy >= 90 THEN 1900 + start_yy ELSE 2000 + start_yy END;
    cutoff_market_date := make_date(start_year, 7, 1);

    DELETE FROM kickbase_raw.fact_player_event AS e
    USING kickbase_raw.dim_match AS m
    WHERE e.match_uid = m.match_uid
      AND (m.season_uid IS NULL OR m.season_uid < min_keep_season_uid);

    DELETE FROM kickbase_raw.fact_player_match AS f
    USING kickbase_raw.dim_match AS m
    WHERE f.match_uid = m.match_uid
      AND (m.season_uid IS NULL OR m.season_uid < min_keep_season_uid);

    DELETE FROM kickbase_raw.dim_match
    WHERE season_uid IS NULL OR season_uid < min_keep_season_uid;

    DELETE FROM kickbase_raw.bridge_player_team
    WHERE season_uid < min_keep_season_uid;

    DELETE FROM kickbase_raw.dim_season
    WHERE season_uid < min_keep_season_uid;

    DELETE FROM kickbase_raw.fact_market_value_daily
    WHERE mv_date < cutoff_market_date;

    DELETE FROM kickbase_raw.dim_team AS t
    WHERE NOT EXISTS (
        SELECT 1
        FROM kickbase_raw.dim_match AS m
        WHERE m.home_team_uid = t.team_uid
           OR m.away_team_uid = t.team_uid
    )
      AND NOT EXISTS (
        SELECT 1
        FROM kickbase_raw.bridge_player_team AS b
        WHERE b.team_uid = t.team_uid
    );
END $$;
