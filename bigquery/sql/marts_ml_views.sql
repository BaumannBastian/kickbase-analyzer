-- ------------------------------------
-- marts_ml_views.sql
--
-- CORE -> MARTS ML Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{marts}.mart_ml_player_predictions` AS
SELECT
  p.ml_run_ts,
  c.champion_model,
  p.snapshot_date,
  p.player_uid,
  p.player_name,
  p.team_uid,
  p.match_uid,
  p.start_probability,
  p.sub_probability,
  p.expected_minutes,
  p.expected_points_next_match,
  p.expected_points_if_full,
  p.expected_raw_points_next_match,
  p.expected_on_top_points_next_match,
  p.raw_points_if_full,
  p.on_top_points_if_full,
  p.raw_exported_at
FROM `{project}.{core}.v_ml_live_predictions` p
LEFT JOIN `{project}.{core}.v_ml_champion_selection` c
  ON c.ml_run_ts = p.ml_run_ts;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_ml_model_monitoring` AS
SELECT
  r.ml_run_ts,
  r.champion_model,
  c.champion_reason,
  r.history_players,
  r.history_rows,
  r.live_rows,
  r.sklearn_points_total_rmse,
  r.torch_points_total_rmse,
  c.metric_key,
  c.sklearn_score AS champion_sklearn_score,
  c.torch_score AS champion_torch_score,
  r.raw_exported_at
FROM `{project}.{core}.v_ml_run_summary` r
LEFT JOIN `{project}.{core}.v_ml_champion_selection` c
  ON c.ml_run_ts = r.ml_run_ts;
