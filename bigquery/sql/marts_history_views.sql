-- ------------------------------------
-- marts_history_views.sql
--
-- CORE -> MARTS History Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{marts}.mart_hist_player_profile` AS
WITH latest_mv AS (
  SELECT
    player_uid,
    mv_date AS current_market_value_date,
    market_value AS current_market_value,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid
      ORDER BY mv_date DESC
    ) AS rn
  FROM `{project}.{core}.v_hist_player_marketvalue_daily`
)
SELECT
  p.player_uid,
  p.player_name,
  p.player_birthdate,
  CASE
    WHEN p.player_birthdate IS NULL THEN NULL
    ELSE DATE_DIFF(CURRENT_DATE(), p.player_birthdate, YEAR)
      - IF(FORMAT_DATE('%m%d', CURRENT_DATE()) < FORMAT_DATE('%m%d', p.player_birthdate), 1, 0)
  END AS player_age_years,
  p.player_position,
  p.team_uid,
  p.team_name,
  p.kickbase_team_id,
  p.kb_player_id,
  p.ligainsider_player_id,
  p.ligainsider_player_slug,
  p.ligainsider_name,
  p.ligainsider_profile_url,
  p.image_mime,
  p.image_sha256,
  p.image_local_path,
  mv.current_market_value_date,
  mv.current_market_value,
  p.player_updated_at,
  p.source_snapshot_ts,
  p.raw_exported_at
FROM `{project}.{core}.v_hist_player_profile_latest` p
LEFT JOIN latest_mv mv
  ON mv.player_uid = p.player_uid
 AND mv.rn = 1;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_hist_player_marketvalue_curve` AS
SELECT
  mv.player_uid,
  p.player_name,
  p.team_uid,
  p.team_name,
  mv.mv_date,
  mv.market_value,
  mv.source_dt_days,
  mv.ingested_at,
  mv.source_snapshot_ts,
  mv.raw_exported_at
FROM `{project}.{core}.v_hist_player_marketvalue_daily` mv
LEFT JOIN `{project}.{core}.v_hist_player_profile_latest` p
  ON p.player_uid = mv.player_uid;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_hist_player_match_breakdown` AS
WITH component_rollup AS (
  SELECT
    player_uid,
    match_uid,
    ARRAY_AGG(
      STRUCT(
        event_type_id,
        COALESCE(event_name, CONCAT('event_', CAST(event_type_id AS STRING))) AS event_name,
        event_count,
        points_sum
      )
      ORDER BY ABS(points_sum) DESC, COALESCE(event_name, CONCAT('event_', CAST(event_type_id AS STRING)))
    ) AS component_list,
    STRING_AGG(
      CONCAT(
        COALESCE(event_name, CONCAT('event_', CAST(event_type_id AS STRING))),
        ' (',
        CAST(points_sum AS STRING),
        ')'
      ),
      ', '
      ORDER BY ABS(points_sum) DESC, COALESCE(event_name, CONCAT('event_', CAST(event_type_id AS STRING)))
    ) AS component_brackets,
    SUM(points_sum) AS component_points_total
  FROM `{project}.{core}.v_hist_player_match_components`
  GROUP BY
    player_uid,
    match_uid
)
SELECT
  m.player_uid,
  p.player_name,
  p.team_uid,
  p.team_name,
  m.match_uid,
  m.season_uid,
  m.season_label,
  m.matchday,
  m.kickoff_ts,
  m.home_team_uid,
  m.home_team_name,
  m.away_team_uid,
  m.away_team_name,
  m.team_uid AS player_team_uid,
  m.opponent_team_uid,
  m.points_total,
  m.is_home,
  m.match_result,
  m.score_home,
  m.score_away,
  r.component_points_total,
  r.component_brackets,
  r.component_list,
  m.source_snapshot_ts,
  m.raw_exported_at
FROM `{project}.{core}.v_hist_player_match_summary` m
LEFT JOIN `{project}.{core}.v_hist_player_profile_latest` p
  ON p.player_uid = m.player_uid
LEFT JOIN component_rollup r
  ON r.player_uid = m.player_uid
 AND r.match_uid = m.match_uid;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_hist_player_comparison` AS
WITH latest_mv AS (
  SELECT
    player_uid,
    market_value,
    mv_date,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid
      ORDER BY mv_date DESC
    ) AS rn
  FROM `{project}.{core}.v_hist_player_marketvalue_daily`
),
recent_matches AS (
  SELECT
    player_uid,
    points_total,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid
      ORDER BY kickoff_ts DESC, season_uid DESC, matchday DESC, match_uid DESC
    ) AS rn
  FROM `{project}.{core}.v_hist_player_match_summary`
),
recent_stats AS (
  SELECT
    player_uid,
    AVG(IF(rn <= 5, points_total, NULL)) AS avg_points_last_5,
    AVG(IF(rn <= 10, points_total, NULL)) AS avg_points_last_10,
    COUNTIF(rn <= 5) AS matches_last_5,
    COUNTIF(rn <= 10) AS matches_last_10
  FROM recent_matches
  GROUP BY player_uid
)
SELECT
  p.player_uid,
  p.player_name,
  p.player_position,
  p.player_birthdate,
  p.player_age_years,
  p.team_uid,
  p.team_name,
  p.kb_player_id,
  p.ligainsider_player_id,
  p.current_market_value,
  p.current_market_value_date,
  rs.avg_points_last_5,
  rs.avg_points_last_10,
  rs.matches_last_5,
  rs.matches_last_10,
  p.source_snapshot_ts,
  p.raw_exported_at
FROM `{project}.{marts}.mart_hist_player_profile` p
LEFT JOIN recent_stats rs
  ON rs.player_uid = p.player_uid
LEFT JOIN latest_mv mv
  ON mv.player_uid = p.player_uid
 AND mv.rn = 1;

CREATE OR REPLACE VIEW `{project}.{marts}.mart_hist_team_outlook` AS
WITH lineup_agg AS (
  SELECT
    team_uid,
    COUNTIF(predicted_lineup = 'Safe Starter') AS safe_starter_count,
    COUNTIF(predicted_lineup = 'Potential Starter') AS potential_starter_count,
    COUNTIF(predicted_lineup = 'Bench') AS bench_count,
    STRING_AGG(IF(predicted_lineup = 'Safe Starter', player_name, NULL), ', ' ORDER BY player_name) AS safe_starter_players,
    STRING_AGG(IF(predicted_lineup = 'Potential Starter', player_name, NULL), ', ' ORDER BY player_name) AS potential_starter_players,
    STRING_AGG(IF(predicted_lineup = 'Bench', player_name, NULL), ', ' ORDER BY player_name) AS bench_players,
    MAX(source_snapshot_ts) AS lineup_snapshot_ts,
    MAX(raw_exported_at) AS lineup_exported_at
  FROM `{project}.{core}.v_hist_team_lineup_players_latest`
  WHERE team_uid IS NOT NULL
  GROUP BY team_uid
),
odds_expanded AS (
  SELECT
    odds_event_id,
    commence_time,
    odds_last_changed_at,
    odds_collected_at,
    home_team_uid AS team_uid,
    away_team_uid AS opponent_team_uid,
    home_team_name AS team_name_from_odds,
    away_team_name AS opponent_team_name_from_odds,
    'home' AS venue,
    h2h_home_odds AS team_win_odds,
    h2h_draw_odds AS draw_odds,
    h2h_away_odds AS opponent_win_odds,
    h2h_home_implied_prob AS team_win_implied_prob,
    h2h_draw_implied_prob AS draw_implied_prob,
    h2h_away_implied_prob AS opponent_win_implied_prob,
    totals_line,
    totals_over_odds,
    totals_under_odds,
    totals_over_implied_prob,
    totals_under_implied_prob,
    source_snapshot_ts,
    raw_exported_at
  FROM `{project}.{core}.v_hist_team_odds_snapshot`

  UNION ALL

  SELECT
    odds_event_id,
    commence_time,
    odds_last_changed_at,
    odds_collected_at,
    away_team_uid AS team_uid,
    home_team_uid AS opponent_team_uid,
    away_team_name AS team_name_from_odds,
    home_team_name AS opponent_team_name_from_odds,
    'away' AS venue,
    h2h_away_odds AS team_win_odds,
    h2h_draw_odds AS draw_odds,
    h2h_home_odds AS opponent_win_odds,
    h2h_away_implied_prob AS team_win_implied_prob,
    h2h_draw_implied_prob AS draw_implied_prob,
    h2h_home_implied_prob AS opponent_win_implied_prob,
    totals_line,
    totals_over_odds,
    totals_under_odds,
    totals_over_implied_prob,
    totals_under_implied_prob,
    source_snapshot_ts,
    raw_exported_at
  FROM `{project}.{core}.v_hist_team_odds_snapshot`
),
next_odds AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY team_uid
      ORDER BY commence_time ASC, odds_last_changed_at DESC
    ) AS rn
  FROM odds_expanded
  WHERE team_uid IS NOT NULL
    AND commence_time IS NOT NULL
    AND commence_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT
  t.team_uid,
  t.team_name,
  n.odds_event_id,
  n.commence_time AS next_match_ts,
  n.venue,
  n.opponent_team_uid,
  opp.team_name AS opponent_team_name,
  n.team_win_odds,
  n.draw_odds,
  n.opponent_win_odds,
  n.team_win_implied_prob,
  n.draw_implied_prob,
  n.opponent_win_implied_prob,
  CASE
    WHEN n.team_win_implied_prob >= n.draw_implied_prob
     AND n.team_win_implied_prob >= n.opponent_win_implied_prob THEN 'Win Favored'
    WHEN n.opponent_win_implied_prob >= n.team_win_implied_prob
     AND n.opponent_win_implied_prob >= n.draw_implied_prob THEN 'Loss Favored'
    ELSE 'Draw Lean'
  END AS expected_result_bucket,
  n.totals_line,
  n.totals_over_odds,
  n.totals_under_odds,
  n.totals_over_implied_prob,
  n.totals_under_implied_prob,
  l.safe_starter_count,
  l.potential_starter_count,
  l.bench_count,
  l.safe_starter_players,
  l.potential_starter_players,
  l.bench_players,
  n.source_snapshot_ts AS odds_snapshot_ts,
  l.lineup_snapshot_ts,
  n.raw_exported_at AS odds_exported_at,
  l.lineup_exported_at
FROM `{project}.{core}.v_hist_team_latest` t
LEFT JOIN next_odds n
  ON n.team_uid = t.team_uid
 AND n.rn = 1
LEFT JOIN `{project}.{core}.v_hist_team_latest` opp
  ON opp.team_uid = n.opponent_team_uid
LEFT JOIN lineup_agg l
  ON l.team_uid = t.team_uid;
