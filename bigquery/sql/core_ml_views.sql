-- ------------------------------------
-- core_ml_views.sql
--
-- RAW -> CORE ML Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{core}.v_ml_live_predictions` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, snapshot_date
      ORDER BY ml_run_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.ml_live_predictions`
)
SELECT
  ml_run_ts,
  champion_model,
  player_uid,
  player_name,
  team_uid,
  match_uid,
  snapshot_date,
  start_probability,
  sub_probability,
  expected_minutes,
  expected_points_next_match,
  expected_points_if_full,
  expected_raw_points_next_match,
  expected_on_top_points_next_match,
  raw_points_if_full,
  on_top_points_if_full,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_ml_cv_fold_metrics` AS
SELECT
  ml_run_ts,
  model_family,
  fold,
  metric_points_total_rmse,
  metric_points_total_mae,
  metric_points_total_bias,
  metric_raw_points_rmse,
  metric_raw_points_mae,
  metric_raw_points_bias,
  metric_on_top_points_rmse,
  metric_on_top_points_mae,
  metric_on_top_points_bias,
  metric_start_probability_brier,
  metric_sub_probability_brier,
  train_rows,
  valid_rows,
  raw_exported_at
FROM `{project}.{raw}.ml_cv_fold_metrics`;

CREATE OR REPLACE VIEW `{project}.{core}.v_ml_champion_selection` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ml_run_ts
      ORDER BY raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.ml_champion_selection`
)
SELECT
  ml_run_ts,
  champion_model,
  champion_reason,
  metric_key,
  sklearn_score,
  torch_score,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_ml_run_summary` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ml_run_ts
      ORDER BY raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.ml_run_summary`
)
SELECT
  ml_run_ts,
  history_players,
  history_rows,
  live_rows,
  champion_model,
  sklearn_points_total_rmse,
  torch_points_total_rmse,
  run_summary,
  raw_exported_at
FROM dedup
WHERE rn = 1;
