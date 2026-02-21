-- ------------------------------------
-- marts_views.sql
--
-- CORE -> MARTS Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{marts}.mart_player_leaderboard` AS
SELECT
  snapshot_date,
  player_uid,
  player_name,
  team,
  position,
  start_probability,
  expected_points_next_matchday,
  market_value,
  expected_marketvalue_next_matchday,
  expected_marketvalue_change_7d,
  value_score,
  source_snapshot_ts,
  raw_exported_at
FROM `{project}.{core}.v_feat_player_daily`
ORDER BY value_score DESC;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_points_breakdown` AS
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
FROM `{project}.{core}.v_points_components_matchday`;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_risk_overview` AS
SELECT
  snapshot_date,
  player_uid,
  player_name,
  p_dnp,
  stddev_points,
  p10_points,
  p50_points,
  p90_points,
  source_snapshot_ts,
  raw_exported_at
FROM `{project}.{core}.v_feat_player_daily`;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_marketvalue_dashboard` AS
SELECT
  snapshot_date,
  player_uid,
  player_name,
  market_value,
  expected_marketvalue_next_matchday,
  expected_marketvalue_change_7d,
  value_score,
  source_snapshot_ts,
  raw_exported_at
FROM `{project}.{core}.v_feat_player_daily`;
