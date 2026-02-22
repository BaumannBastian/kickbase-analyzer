# ------------------------------------
# db.py
#
# PostgreSQL-Zugriffsschicht fuer Kickbase-History-ETL.
# Enthalten sind Verbindungsaufbau, Upserts und State-Handling.
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
from typing import Any, Sequence

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


@dataclass(frozen=True)
class ExistingPlayerIdentity:
    player_uid: str
    kb_player_id: int
    birth_date: date | None
    ligainsider_player_slug: str | None
    ligainsider_player_id: int | None
    player_image_url: str | None


def get_connection(config: DbConfig) -> PgConnection:
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=config.connect_timeout,
    )


def get_existing_player_identity_by_kb_player_id(
    conn: PgConnection,
    kb_player_id: int,
) -> ExistingPlayerIdentity | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                player_uid,
                kb_player_id,
                birth_date,
                ligainsider_player_slug,
                ligainsider_player_id,
                player_image_url
            FROM dim_players
            WHERE kb_player_id = %s
            """,
            (kb_player_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    return ExistingPlayerIdentity(
        player_uid=str(row[0]),
        kb_player_id=int(row[1]),
        birth_date=row[2],
        ligainsider_player_slug=_to_text_or_none(row[3]),
        ligainsider_player_id=_to_int_or_none(row[4]),
        player_image_url=_to_text_or_none(row[5]),
    )


def upsert_players(conn: PgConnection, players: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not players:
        return 0, 0

    values = [
        (
            str(player["player_uid"]),
            int(player["kb_player_id"]),
            str(player.get("name") or "").strip() or f"player_{player['kb_player_id']}",
            player.get("birth_date"),
            _to_text_or_none(player.get("ligainsider_player_slug")),
            _to_int_or_none(player.get("ligainsider_player_id")),
            _to_int_or_none(player.get("team_id")),
            _to_text_or_none(player.get("team_code")),
            _to_text_or_none(player.get("team_name")),
            _to_text_or_none(player.get("player_image_url")),
            _to_text_or_none(player.get("position")),
            _to_int_or_none(player.get("competition_id")),
        )
        for player in players
    ]

    sql = """
        INSERT INTO dim_players (
            player_uid,
            kb_player_id,
            name,
            birth_date,
            ligainsider_player_slug,
            ligainsider_player_id,
            team_id,
            team_code,
            team_name,
            player_image_url,
            position,
            competition_id
        )
        VALUES %s
        ON CONFLICT (kb_player_id)
        DO UPDATE SET
            player_uid = EXCLUDED.player_uid,
            name = EXCLUDED.name,
            birth_date = COALESCE(EXCLUDED.birth_date, dim_players.birth_date),
            ligainsider_player_slug = COALESCE(EXCLUDED.ligainsider_player_slug, dim_players.ligainsider_player_slug),
            ligainsider_player_id = COALESCE(EXCLUDED.ligainsider_player_id, dim_players.ligainsider_player_id),
            team_id = COALESCE(EXCLUDED.team_id, dim_players.team_id),
            team_code = COALESCE(EXCLUDED.team_code, dim_players.team_code),
            team_name = COALESCE(EXCLUDED.team_name, dim_players.team_name),
            player_image_url = COALESCE(EXCLUDED.player_image_url, dim_players.player_image_url),
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


def get_max_market_value_date(conn: PgConnection, player_uid: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(mv_date)
            FROM fact_market_value
            WHERE player_uid = %s
            """,
            (player_uid,),
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
            str(row["player_uid"]),
            int(row["kb_player_id"]),
            row["mv_date"],
            int(row["market_value"]),
            _to_int_or_none(row.get("source_dt_days")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_market_value
            (player_uid, kb_player_id, mv_date, market_value, source_dt_days)
        VALUES %s
        ON CONFLICT (player_uid, mv_date)
        DO UPDATE SET
            kb_player_id = EXCLUDED.kb_player_id,
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
            str(row["player_uid"]),
            int(row["kb_player_id"]),
            int(row["competition_id"]),
            str(row.get("season_label") or "unknown"),
            int(row["matchday"]),
            str(row["match_uid"]),
            _to_int_or_none(row.get("match_id")),
            _to_int_or_none(row.get("team_id")),
            _to_int_or_none(row.get("opponent_team_id")),
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
            player_uid,
            kb_player_id,
            competition_id,
            season_label,
            matchday,
            match_uid,
            match_id,
            team_id,
            opponent_team_id,
            is_home,
            score_home,
            score_away,
            match_result,
            points_total,
            raw_json
        )
        VALUES %s
        ON CONFLICT (player_uid, competition_id, season_label, matchday)
        DO UPDATE SET
            kb_player_id = EXCLUDED.kb_player_id,
            match_uid = EXCLUDED.match_uid,
            match_id = COALESCE(EXCLUDED.match_id, fact_match_performance.match_id),
            team_id = COALESCE(EXCLUDED.team_id, fact_match_performance.team_id),
            opponent_team_id = COALESCE(EXCLUDED.opponent_team_id, fact_match_performance.opponent_team_id),
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
            str(row["player_uid"]),
            int(row["kb_player_id"]),
            int(row["competition_id"]),
            str(row.get("season_label") or "unknown"),
            int(row["matchday"]),
            _to_text_or_none(row.get("match_uid")),
            int(row["event_type_id"]),
            _to_text_or_none(row.get("event_name")),
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
            player_uid,
            kb_player_id,
            competition_id,
            season_label,
            matchday,
            match_uid,
            event_type_id,
            event_name,
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


def cleanup_unknown_event_rows_for_player(
    conn: PgConnection,
    *,
    player_uid: str,
    competition_id: int,
    season_label: str,
) -> int:
    deleted_events = 0

    delete_events_sql = """
        DELETE FROM fact_match_events unknown_rows
        USING fact_match_events known_rows
        WHERE known_rows.player_uid = %s
          AND known_rows.competition_id = %s
          AND known_rows.season_label = %s
          AND unknown_rows.player_uid = known_rows.player_uid
          AND unknown_rows.competition_id = known_rows.competition_id
          AND unknown_rows.matchday = known_rows.matchday
          AND unknown_rows.event_type_id = known_rows.event_type_id
          AND unknown_rows.points = known_rows.points
          AND COALESCE(unknown_rows.mt, -1) = COALESCE(known_rows.mt, -1)
          AND COALESCE(unknown_rows.att, '') = COALESCE(known_rows.att, '')
          AND unknown_rows.season_label = 'unknown'
    """

    with conn.cursor() as cur:
        cur.execute(delete_events_sql, (player_uid, competition_id, season_label))
        deleted_events = cur.rowcount

    return deleted_events


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
