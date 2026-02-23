-- ------------------------------------
-- core_history_views.sql
--
-- RAW -> CORE History Views fuer Kickbase Analyzer.
-- Platzhalter (werden zur Laufzeit ersetzt):
--   {project}, {raw}, {core}, {marts}
-- ------------------------------------

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_team_latest` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY team_uid
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_team_snapshot`
)
SELECT
  team_uid,
  team_name,
  kickbase_team_id,
  ligainsider_team_url,
  updated_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_player_profile_latest` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_player_profile_snapshot`
)
SELECT
  player_uid,
  player_name,
  player_birthdate,
  player_position,
  team_uid,
  team_name,
  kickbase_team_id,
  kb_player_id,
  ligainsider_player_id,
  ligainsider_player_slug,
  ligainsider_name,
  ligainsider_profile_url,
  image_mime,
  image_sha256,
  image_local_path,
  player_updated_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_player_marketvalue_daily` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, mv_date
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_player_marketvalue_daily`
)
SELECT
  player_uid,
  mv_date,
  market_value,
  source_dt_days,
  ingested_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_player_match_summary` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, match_uid
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_player_match_summary`
)
SELECT
  player_uid,
  match_uid,
  points_total,
  is_home,
  match_result,
  league_key,
  season_uid,
  season_label,
  matchday,
  kickoff_ts,
  home_team_uid,
  home_team_name,
  away_team_uid,
  away_team_name,
  score_home,
  score_away,
  team_uid,
  opponent_team_uid,
  ingested_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_player_match_components` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY player_uid, match_uid, event_type_id
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_player_match_components`
)
SELECT
  player_uid,
  match_uid,
  event_type_id,
  event_name,
  event_count,
  points_sum,
  first_ingested_at,
  last_ingested_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_team_lineup_players_latest` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(team_uid, 'UNKNOWN_TEAM'),
        COALESCE(CAST(ligainsider_player_id AS STRING), player_name)
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_team_lineup_players`
)
SELECT
  team_uid,
  team_source_url,
  ligainsider_player_id,
  ligainsider_player_slug,
  ligainsider_profile_url,
  player_name,
  predicted_lineup,
  status,
  competition_player_count,
  competition_player_names,
  scraped_at,
  ingested_at,
  last_changed_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;

CREATE OR REPLACE VIEW `{project}.{core}.v_hist_team_odds_snapshot` AS
WITH dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(odds_event_id, CONCAT(COALESCE(home_team_name, ''), '|', COALESCE(away_team_name, ''), '|', COALESCE(CAST(commence_time AS STRING), '')))
      ORDER BY source_snapshot_ts DESC, raw_exported_at DESC
    ) AS rn
  FROM `{project}.{raw}.hist_team_odds_snapshot`
)
SELECT
  odds_event_id,
  sport_key,
  sport_title,
  commence_time,
  odds_last_changed_at,
  odds_collected_at,
  home_team_name,
  away_team_name,
  home_team_uid,
  away_team_uid,
  h2h_home_odds,
  h2h_draw_odds,
  h2h_away_odds,
  h2h_home_implied_prob,
  h2h_draw_implied_prob,
  h2h_away_implied_prob,
  totals_line,
  totals_over_odds,
  totals_under_odds,
  totals_over_implied_prob,
  totals_under_implied_prob,
  bookmaker_count,
  ingested_at,
  source_snapshot_ts,
  raw_exported_at
FROM dedup
WHERE rn = 1;
