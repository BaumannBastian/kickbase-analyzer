-- ------------------------------------
-- V3__add_match_context_fields.sql
--
-- Ergaenzt Match-Performance um Home/Away-Flag und Match-Resultat
-- fuer direkte W/D/L-Auswertungen ohne Zusatzlogik im BI-Layer.
-- ------------------------------------

ALTER TABLE fact_match_performance
    ADD COLUMN IF NOT EXISTS is_home BOOLEAN NULL;

ALTER TABLE fact_match_performance
    ADD COLUMN IF NOT EXISTS match_result CHAR(1) NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_fact_match_performance_match_result'
    ) THEN
        ALTER TABLE fact_match_performance
            ADD CONSTRAINT chk_fact_match_performance_match_result
            CHECK (match_result IN ('W', 'D', 'L') OR match_result IS NULL);
    END IF;
END $$;

UPDATE fact_match_performance
SET
    is_home = CASE
        WHEN team_id IS NULL THEN NULL
        WHEN (raw_json->>'t1') ~ '^-?[0-9]+$' AND team_id = (raw_json->>'t1')::BIGINT THEN TRUE
        WHEN (raw_json->>'t2') ~ '^-?[0-9]+$' AND team_id = (raw_json->>'t2')::BIGINT THEN FALSE
        ELSE is_home
    END
WHERE is_home IS NULL
  AND raw_json IS NOT NULL;

UPDATE fact_match_performance
SET
    match_result = CASE
        WHEN score_home IS NULL OR score_away IS NULL OR is_home IS NULL THEN NULL
        WHEN is_home = TRUE AND score_home > score_away THEN 'W'
        WHEN is_home = TRUE AND score_home = score_away THEN 'D'
        WHEN is_home = TRUE AND score_home < score_away THEN 'L'
        WHEN is_home = FALSE AND score_away > score_home THEN 'W'
        WHEN is_home = FALSE AND score_away = score_home THEN 'D'
        WHEN is_home = FALSE AND score_away < score_home THEN 'L'
        ELSE NULL
    END
WHERE match_result IS NULL;

CREATE INDEX IF NOT EXISTS idx_fact_match_performance_result
    ON fact_match_performance (player_id, competition_id, season_label, matchday DESC, match_result);
