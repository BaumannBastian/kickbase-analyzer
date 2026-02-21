-- ------------------------------------
-- core_views.sql
--
-- RAW -> CORE Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{core}.v_feat_player_daily` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, snapshot_date
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.feat_player_daily`
)
SELECT
  player_uid,
  player_name,
  team,
  position,
  snapshot_date,
  start_probability,
  expected_points_next_matchday,
  p_dnp,
  stddev_points,
  p10_points,
  p50_points,
  p90_points,
  market_value,
  expected_marketvalue_next_matchday,
  expected_marketvalue_change_7d,
  value_score,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_points_components_matchday` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, matchday
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.points_components_matchday`
)
SELECT
  player_uid,
  player_name,
  matchday,
  base_raw_ev,
  scorer_ev,
  win_ev,
  minutes_bonus_ev,
  cards_negative_ev,
  pred_total,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_quality_metrics` AS
SELECT
  metric,
  value,
  timestamp,
  source_snapshot_ts,
  raw_exported_at
FROM `{project}.{raw}.quality_metrics`;
