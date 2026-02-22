-- ------------------------------------
-- V9__team_uid_text_and_merge_prep.sql
--
-- Migriert team_uid auf sprechende Text-Keys (z.B. RBL, BVB)
-- und passt referenzierende Tabellen/FKs entsprechend an.
-- ------------------------------------

ALTER TABLE kickbase_raw.dim_team
    ADD COLUMN team_uid_v2 TEXT;

UPDATE kickbase_raw.dim_team
SET team_uid_v2 = UPPER(
    COALESCE(
        NULLIF(team_code, ''),
        'T' || COALESCE(kickbase_team_id::text, team_uid::text)
    )
);

WITH ranked AS (
    SELECT
        ctid,
        team_uid_v2,
        league_key,
        ROW_NUMBER() OVER (
            PARTITION BY team_uid_v2
            ORDER BY league_key, kickbase_team_id NULLS LAST
        ) AS rn
    FROM kickbase_raw.dim_team
)
UPDATE kickbase_raw.dim_team AS t
SET team_uid_v2 = r.team_uid_v2 || '_' || UPPER(REGEXP_REPLACE(r.league_key, '[^A-Za-z0-9]+', '', 'g'))
FROM ranked AS r
WHERE t.ctid = r.ctid
  AND r.rn > 1;

ALTER TABLE kickbase_raw.dim_team
    ALTER COLUMN team_uid_v2 SET NOT NULL;

ALTER TABLE kickbase_raw.bridge_player_team
    ADD COLUMN team_uid_v2 TEXT;

UPDATE kickbase_raw.bridge_player_team AS b
SET team_uid_v2 = t.team_uid_v2
FROM kickbase_raw.dim_team AS t
WHERE t.team_uid = b.team_uid;

ALTER TABLE kickbase_raw.dim_match
    ADD COLUMN home_team_uid_v2 TEXT,
    ADD COLUMN away_team_uid_v2 TEXT;

UPDATE kickbase_raw.dim_match AS m
SET home_team_uid_v2 = t.team_uid_v2
FROM kickbase_raw.dim_team AS t
WHERE t.team_uid = m.home_team_uid;

UPDATE kickbase_raw.dim_match AS m
SET away_team_uid_v2 = t.team_uid_v2
FROM kickbase_raw.dim_team AS t
WHERE t.team_uid = m.away_team_uid;

ALTER TABLE kickbase_raw.bridge_player_team
    DROP CONSTRAINT IF EXISTS bridge_player_team_team_uid_fkey;

ALTER TABLE kickbase_raw.dim_match
    DROP CONSTRAINT IF EXISTS dim_match_home_team_uid_fkey,
    DROP CONSTRAINT IF EXISTS dim_match_away_team_uid_fkey;

ALTER TABLE kickbase_raw.dim_team
    DROP CONSTRAINT IF EXISTS dim_team_pkey;

ALTER TABLE kickbase_raw.bridge_player_team
    DROP COLUMN team_uid;

ALTER TABLE kickbase_raw.bridge_player_team
    RENAME COLUMN team_uid_v2 TO team_uid;

ALTER TABLE kickbase_raw.bridge_player_team
    ALTER COLUMN team_uid SET NOT NULL;

ALTER TABLE kickbase_raw.dim_match
    DROP COLUMN home_team_uid,
    DROP COLUMN away_team_uid;

ALTER TABLE kickbase_raw.dim_match
    RENAME COLUMN home_team_uid_v2 TO home_team_uid;

ALTER TABLE kickbase_raw.dim_match
    RENAME COLUMN away_team_uid_v2 TO away_team_uid;

ALTER TABLE kickbase_raw.dim_team
    DROP COLUMN team_uid;

ALTER TABLE kickbase_raw.dim_team
    RENAME COLUMN team_uid_v2 TO team_uid;

ALTER TABLE kickbase_raw.dim_team
    ALTER COLUMN team_uid SET NOT NULL;

ALTER TABLE kickbase_raw.dim_team
    ADD CONSTRAINT dim_team_pkey PRIMARY KEY (team_uid);

ALTER TABLE kickbase_raw.bridge_player_team
    ADD CONSTRAINT bridge_player_team_team_uid_fkey
    FOREIGN KEY (team_uid)
    REFERENCES kickbase_raw.dim_team(team_uid)
    ON UPDATE CASCADE
    ON DELETE RESTRICT;

ALTER TABLE kickbase_raw.dim_match
    ADD CONSTRAINT dim_match_home_team_uid_fkey
    FOREIGN KEY (home_team_uid)
    REFERENCES kickbase_raw.dim_team(team_uid)
    ON UPDATE CASCADE
    ON DELETE SET NULL;

ALTER TABLE kickbase_raw.dim_match
    ADD CONSTRAINT dim_match_away_team_uid_fkey
    FOREIGN KEY (away_team_uid)
    REFERENCES kickbase_raw.dim_team(team_uid)
    ON UPDATE CASCADE
    ON DELETE SET NULL;
