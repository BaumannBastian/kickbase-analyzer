-- ------------------------------------
-- V11__normalize_team_uids_and_match_uids.sql
--
-- Ersetzt legacy Team-UIDs im Format T<id> durch stabile
-- 3/4-stellige Teamkuerzel und schreibt Match-UIDs neu.
-- ------------------------------------

WITH team_mapping (kickbase_team_id, team_code, team_full_name) AS (
    VALUES
        (2, 'FCB', 'FC Bayern'),
        (3, 'BVB', 'Borussia Dortmund'),
        (4, 'SGE', 'Eintracht Frankfurt'),
        (5, 'SCF', 'SC Freiburg'),
        (6, 'HSV', 'Hamburger SV'),
        (7, 'B04', 'Bayer Leverkusen'),
        (8, 'S04', 'Schalke 04'),
        (9, 'VFB', 'VfB Stuttgart'),
        (10, 'SVW', 'Werder Bremen'),
        (11, 'WOB', 'VfL Wolfsburg'),
        (12, 'F95', 'Fortuna Duesseldorf'),
        (13, 'FCA', 'FC Augsburg'),
        (14, 'TSG', 'TSG Hoffenheim'),
        (15, 'BMG', 'Bor. M''gladbach'),
        (16, 'FCN', '1. FC Nuernberg'),
        (17, 'H96', 'Hannover 96'),
        (18, 'M05', 'Mainz 05'),
        (19, 'SGF', 'SpVgg Greuther Fuerth'),
        (20, 'BSC', 'Hertha BSC'),
        (21, 'EBS', 'Eintracht Braunschweig'),
        (22, 'DSC', 'Arminia Bielefeld'),
        (23, 'HRO', 'Hansa Rostock'),
        (24, 'BOC', 'VfL Bochum'),
        (27, 'KSC', 'Karlsruher SC'),
        (28, 'KOE', '1. FC Koeln'),
        (29, 'SCP', 'SC Paderborn'),
        (30, 'FCK', '1. FC Kaiserslautern'),
        (32, 'AUE', 'Erzgebirge Aue'),
        (35, 'SVS', 'SV Sandhausen'),
        (36, 'FCI', 'FC Ingolstadt 04'),
        (39, 'STP', 'FC St. Pauli'),
        (40, 'FCU', 'Union Berlin'),
        (41, 'SGD', 'Dynamo Dresden'),
        (42, 'D98', 'SV Darmstadt 98'),
        (43, 'RBL', 'RB Leipzig'),
        (47, 'REG', 'SSV Jahn Regensburg'),
        (48, 'FCM', '1. FC Magdeburg'),
        (49, 'HEI', '1. FC Heidenheim 1846'),
        (50, 'FCH', '1. FC Heidenheim'),
        (51, 'KIE', 'Holstein Kiel'),
        (75, 'WIE', 'SV Wehen Wiesbaden'),
        (76, 'OSN', 'VfL Osnabrueck'),
        (77, 'ELV', 'SV Elversberg'),
        (93, 'ULM', 'SSV Ulm 1846'),
        (94, 'MUE', 'Preussen Muenster')
)
UPDATE kickbase_raw.dim_team AS t
SET
    team_uid = m.team_code,
    team_code = m.team_code,
    team_name = m.team_code || ' (' || m.team_full_name || ')',
    updated_at = now()
FROM team_mapping AS m
WHERE t.kickbase_team_id = m.kickbase_team_id;

UPDATE kickbase_raw.dim_match
SET
    match_uid = (
        CASE
            WHEN regexp_replace(COALESCE(season_label, ''), '\s+', '', 'g') ~ '^[0-9]{4}/[0-9]{4}$'
                THEN right(split_part(regexp_replace(season_label, '\s+', '', 'g'), '/', 1), 2)
                     || '/'
                     || right(split_part(regexp_replace(season_label, '\s+', '', 'g'), '/', 2), 2)
            ELSE COALESCE(NULLIF(trim(season_label), ''), 'unknown')
        END
        || '-MD'
        || lpad(matchday::text, 2, '0')
        || '-'
        || COALESCE(home_team_uid, 'TNA')
        || COALESCE(away_team_uid, 'TNA')
    ),
    updated_at = now();

ALTER TABLE kickbase_raw.dim_team
    DROP CONSTRAINT IF EXISTS ck_dim_team_uid_not_legacy_t;

ALTER TABLE kickbase_raw.dim_team
    ADD CONSTRAINT ck_dim_team_uid_not_legacy_t
    CHECK (team_uid !~ '^T[0-9]+$');
