# ------------------------------------
# db.py
#
# PostgreSQL-Zugriffsschicht fuer Kickbase-History-ETL.
# Enthalten sind Verbindungsaufbau, Upserts und Aggregationen.
#
# Usage
# ------------------------------------
# - conn = get_connection(DbConfig.from_env())
# - upsert_market_values(conn, rows)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import os
from typing import Any, Iterable, Sequence

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json, execute_values


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    connect_timeout: int

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("PGHOST", "127.0.0.1"),
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "kickbase_history"),
            user=os.getenv("PGUSER", "kickbase"),
            password=os.getenv("PGPASSWORD", "kickbase"),
            connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "10")),
        )


def get_connection(config: DbConfig) -> PgConnection:
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=config.connect_timeout,
    )


def upsert_players(conn: PgConnection, players: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not players:
        return 0, 0

    values = [
        (
            int(player["player_id"]),
            str(player.get("name") or "").strip() or f"player_{player['player_id']}",
            _to_int_or_none(player.get("team_id")),
            _to_text_or_none(player.get("team_name")),
            _to_text_or_none(player.get("position")),
            _to_int_or_none(player.get("competition_id")),
        )
        for player in players
    ]

    sql = """
        INSERT INTO dim_players
            (player_id, name, team_id, team_name, position, competition_id)
        VALUES %s
        ON CONFLICT (player_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            team_id = COALESCE(EXCLUDED.team_id, dim_players.team_id),
            team_name = COALESCE(EXCLUDED.team_name, dim_players.team_name),
            position = COALESCE(EXCLUDED.position, dim_players.position),
            competition_id = COALESCE(EXCLUDED.competition_id, dim_players.competition_id),
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def upsert_event_types(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            int(row["event_type_id"]),
            str(row.get("name") or f"event_{row['event_type_id']}").strip(),
            _to_text_or_none(row.get("template")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO dim_event_types
            (event_type_id, name, template)
        VALUES %s
        ON CONFLICT (event_type_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            template = EXCLUDED.template,
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def get_max_market_value_date(conn: PgConnection, player_id: int) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(mv_date)
            FROM fact_market_value
            WHERE player_id = %s
            """,
            (player_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row[0]


def upsert_market_values(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            int(row["player_id"]),
            row["mv_date"],
            int(row["market_value"]),
            _to_int_or_none(row.get("source_dt_days")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_market_value
            (player_id, mv_date, market_value, source_dt_days)
        VALUES %s
        ON CONFLICT (player_id, mv_date)
        DO UPDATE SET
            market_value = EXCLUDED.market_value,
            source_dt_days = COALESCE(EXCLUDED.source_dt_days, fact_market_value.source_dt_days),
            ingested_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def upsert_match_performance(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            int(row["player_id"]),
            int(row["competition_id"]),
            str(row.get("season_label") or "unknown"),
            int(row["matchday"]),
            _to_int_or_none(row.get("match_id")),
            row.get("match_ts"),
            _to_int_or_none(row.get("team_id")),
            _to_int_or_none(row.get("opponent_team_id")),
            _to_text_or_none(row.get("opponent_name")),
            _to_bool_or_none(row.get("is_home")),
            _to_int_or_none(row.get("score_home")),
            _to_int_or_none(row.get("score_away")),
            _to_text_or_none(row.get("match_result")),
            int(row["points_total"]),
            Json(row.get("raw_json")) if row.get("raw_json") is not None else None,
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_match_performance (
            player_id,
            competition_id,
            season_label,
            matchday,
            match_id,
            match_ts,
            team_id,
            opponent_team_id,
            opponent_name,
            is_home,
            score_home,
            score_away,
            match_result,
            points_total,
            raw_json
        )
        VALUES %s
        ON CONFLICT (player_id, competition_id, season_label, matchday)
        DO UPDATE SET
            match_id = COALESCE(EXCLUDED.match_id, fact_match_performance.match_id),
            match_ts = COALESCE(EXCLUDED.match_ts, fact_match_performance.match_ts),
            team_id = COALESCE(EXCLUDED.team_id, fact_match_performance.team_id),
            opponent_team_id = COALESCE(EXCLUDED.opponent_team_id, fact_match_performance.opponent_team_id),
            opponent_name = COALESCE(EXCLUDED.opponent_name, fact_match_performance.opponent_name),
            is_home = COALESCE(EXCLUDED.is_home, fact_match_performance.is_home),
            score_home = COALESCE(EXCLUDED.score_home, fact_match_performance.score_home),
            score_away = COALESCE(EXCLUDED.score_away, fact_match_performance.score_away),
            match_result = COALESCE(EXCLUDED.match_result, fact_match_performance.match_result),
            points_total = EXCLUDED.points_total,
            raw_json = EXCLUDED.raw_json,
            ingested_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def insert_match_events(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0

    values = [
        (
            int(row["player_id"]),
            int(row["competition_id"]),
            str(row.get("season_label") or "unknown"),
            int(row["matchday"]),
            int(row["event_type_id"]),
            int(row["points"]),
            _to_int_or_none(row.get("mt")),
            _to_text_or_none(row.get("att")),
            str(row["event_dedup_key"]),
            Json(row.get("raw_event")) if row.get("raw_event") is not None else None,
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_match_events (
            player_id,
            competition_id,
            season_label,
            matchday,
            event_type_id,
            points,
            mt,
            att,
            event_dedup_key,
            raw_event
        )
        VALUES %s
        ON CONFLICT (event_dedup_key) DO NOTHING
        RETURNING 1
    """

    with conn.cursor() as cur:
        inserted_rows = execute_values(cur, sql, values, fetch=True)
    return len(inserted_rows)


def refresh_match_event_agg(
    conn: PgConnection,
    *,
    player_id: int,
    competition_id: int,
    season_label: str,
    matchdays: Iterable[int],
) -> int:
    days = sorted({int(day) for day in matchdays})
    if not days:
        return 0

    sql = """
        INSERT INTO fact_match_event_agg (
            player_id,
            competition_id,
            season_label,
            matchday,
            event_type_id,
            points_sum,
            event_count,
            ingested_at
        )
        SELECT
            e.player_id,
            e.competition_id,
            e.season_label,
            e.matchday,
            e.event_type_id,
            SUM(e.points) AS points_sum,
            COUNT(*) AS event_count,
            now()
        FROM fact_match_events e
        WHERE e.player_id = %s
          AND e.competition_id = %s
          AND e.season_label = %s
          AND e.matchday = ANY(%s)
        GROUP BY e.player_id, e.competition_id, e.season_label, e.matchday, e.event_type_id
        ON CONFLICT (player_id, competition_id, season_label, matchday, event_type_id)
        DO UPDATE SET
            points_sum = EXCLUDED.points_sum,
            event_count = EXCLUDED.event_count,
            ingested_at = now()
        RETURNING 1
    """

    with conn.cursor() as cur:
        cur.execute(sql, (player_id, competition_id, season_label, days))
        rows = cur.fetchall()
    return len(rows)


def cleanup_unknown_event_rows_for_player(
    conn: PgConnection,
    *,
    player_id: int,
    competition_id: int,
    season_label: str,
) -> tuple[int, int]:
    deleted_events = 0
    deleted_agg = 0

    delete_events_sql = """
        DELETE FROM fact_match_events unknown_rows
        USING fact_match_events known_rows
        WHERE known_rows.player_id = %s
          AND known_rows.competition_id = %s
          AND known_rows.season_label = %s
          AND unknown_rows.player_id = known_rows.player_id
          AND unknown_rows.competition_id = known_rows.competition_id
          AND unknown_rows.matchday = known_rows.matchday
          AND unknown_rows.event_type_id = known_rows.event_type_id
          AND unknown_rows.points = known_rows.points
          AND COALESCE(unknown_rows.mt, -1) = COALESCE(known_rows.mt, -1)
          AND COALESCE(unknown_rows.att, '') = COALESCE(known_rows.att, '')
          AND unknown_rows.season_label = 'unknown'
    """

    delete_agg_sql = """
        DELETE FROM fact_match_event_agg unknown_rows
        USING fact_match_event_agg known_rows
        WHERE known_rows.player_id = %s
          AND known_rows.competition_id = %s
          AND known_rows.season_label = %s
          AND unknown_rows.player_id = known_rows.player_id
          AND unknown_rows.competition_id = known_rows.competition_id
          AND unknown_rows.matchday = known_rows.matchday
          AND unknown_rows.event_type_id = known_rows.event_type_id
          AND unknown_rows.points_sum = known_rows.points_sum
          AND unknown_rows.event_count = known_rows.event_count
          AND unknown_rows.season_label = 'unknown'
    """

    with conn.cursor() as cur:
        cur.execute(delete_events_sql, (player_id, competition_id, season_label))
        deleted_events = cur.rowcount
        cur.execute(delete_agg_sql, (player_id, competition_id, season_label))
        deleted_agg = cur.rowcount

    return deleted_events, deleted_agg


def get_state(conn: PgConnection, key: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM etl_state WHERE key = %s", (key,))
        row = cur.fetchone()
    if row is None:
        return None
    return str(row[0])


def set_state(conn: PgConnection, key: str, value: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_state (key, value, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            """,
            (key, value),
        )


def _count_inserted_updated(result: Sequence[Sequence[Any]]) -> tuple[int, int]:
    inserted = 0
    total = 0
    for row in result:
        if not row:
            continue
        total += 1
        inserted_flag = bool(row[0])
        if inserted_flag:
            inserted += 1
    return inserted, total - inserted


def _to_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return None
