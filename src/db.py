# ------------------------------------
# db.py
#
# PostgreSQL-Zugriffsschicht fuer Kickbase-History-ETL
# im RAW-Star-Schema (kickbase_raw).
#
# Usage
# ------------------------------------
# - conn = get_connection(DbConfig.from_env())
# - upsert_dim_players(conn, rows)
# - upsert_market_values(conn, rows)
# ------------------------------------

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
import hashlib
import os
from pathlib import Path
import re
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
    schema: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("PGHOST", "127.0.0.1"),
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "kickbase_history"),
            user=os.getenv("PGUSER", "kickbase"),
            password=os.getenv("PGPASSWORD", "kickbase"),
            connect_timeout=int(os.getenv("PGCONNECT_TIMEOUT", "10")),
            schema=os.getenv("PGSCHEMA", "kickbase_raw"),
        )


@dataclass(frozen=True)
class ExistingPlayerIdentity:
    player_uid: str
    kb_player_id: int | None
    birthdate: date | None
    image_sha256: str | None
    image_mime: str | None
    image_local_path: str | None
    image_blob: bytes | None


@dataclass(frozen=True)
class TeamLookup:
    by_kickbase_team_id: dict[int, str]
    by_team_code: dict[str, str]


def get_connection(config: DbConfig) -> PgConnection:
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        connect_timeout=config.connect_timeout,
        options=f"-c search_path={config.schema},public",
    )


def get_existing_player_identity(
    conn: PgConnection,
    *,
    player_uid: str | None,
    kb_player_id: int | None,
) -> ExistingPlayerIdentity | None:
    with conn.cursor() as cur:
        row = None
        if kb_player_id is not None:
            cur.execute(
                """
                SELECT
                    player_uid,
                    kb_player_id,
                    birthdate,
                    image_sha256,
                    image_mime,
                    image_local_path,
                    image_blob
                FROM dim_player
                WHERE kb_player_id = %s
                """,
                (kb_player_id,),
            )
            row = cur.fetchone()

        if row is None and player_uid:
            cur.execute(
                """
                SELECT
                    player_uid,
                    kb_player_id,
                    birthdate,
                    image_sha256,
                    image_mime,
                    image_local_path,
                    image_blob
                FROM dim_player
                WHERE player_uid = %s
                """,
                (player_uid,),
            )
            row = cur.fetchone()

    if row is None:
        return None

    return ExistingPlayerIdentity(
        player_uid=str(row[0]),
        kb_player_id=_to_int_or_none(row[1]),
        birthdate=row[2],
        image_sha256=_to_text_or_none(row[3]),
        image_mime=_to_text_or_none(row[4]),
        image_local_path=_to_text_or_none(row[5]),
        image_blob=bytes(row[6]) if row[6] is not None else None,
    )


def merge_player_identity(
    conn: PgConnection,
    *,
    source_player_uid: str,
    target_player_uid: str,
    kb_player_id: int | None,
    player_name: str | None = None,
) -> bool:
    source_uid = str(source_player_uid).strip()
    target_uid = str(target_player_uid).strip()
    if not source_uid or not target_uid or source_uid == target_uid:
        return False

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dim_player WHERE player_uid = %s", (source_uid,))
        source_exists = cur.fetchone() is not None
        if not source_exists:
            return False

        # Ensure target exists so FK updates have a valid destination.
        cur.execute(
            """
            INSERT INTO dim_player (player_uid, kb_player_id, player_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (player_uid)
            DO UPDATE SET
                kb_player_id = COALESCE(dim_player.kb_player_id, EXCLUDED.kb_player_id),
                player_name = COALESCE(dim_player.player_name, EXCLUDED.player_name),
                updated_at = now()
            """,
            (target_uid, None, (player_name or target_uid)),
        )

        # Move current kb id away first to avoid unique conflicts while assigning to target.
        if kb_player_id is not None:
            cur.execute(
                """
                UPDATE dim_player
                SET kb_player_id = NULL,
                    updated_at = now()
                WHERE kb_player_id = %s
                  AND player_uid <> %s
                """,
                (kb_player_id, target_uid),
            )

        cur.execute(
            """
            INSERT INTO bridge_player_team (
                player_uid, season_uid, team_uid, valid_from, valid_to, source, created_at, updated_at
            )
            SELECT
                %s, season_uid, team_uid, valid_from, valid_to, source, created_at, now()
            FROM bridge_player_team
            WHERE player_uid = %s
            ON CONFLICT (player_uid, season_uid, source)
            DO UPDATE SET
                team_uid = EXCLUDED.team_uid,
                valid_from = COALESCE(EXCLUDED.valid_from, bridge_player_team.valid_from),
                valid_to = COALESCE(EXCLUDED.valid_to, bridge_player_team.valid_to),
                updated_at = now()
            """,
            (target_uid, source_uid),
        )
        cur.execute("DELETE FROM bridge_player_team WHERE player_uid = %s", (source_uid,))

        cur.execute(
            """
            INSERT INTO fact_market_value_daily (player_uid, mv_date, market_value, source_dt_days, ingested_at)
            SELECT
                %s, mv_date, market_value, source_dt_days, ingested_at
            FROM fact_market_value_daily
            WHERE player_uid = %s
            ON CONFLICT (player_uid, mv_date)
            DO UPDATE SET
                market_value = EXCLUDED.market_value,
                source_dt_days = COALESCE(EXCLUDED.source_dt_days, fact_market_value_daily.source_dt_days),
                ingested_at = now()
            """,
            (target_uid, source_uid),
        )
        cur.execute("DELETE FROM fact_market_value_daily WHERE player_uid = %s", (source_uid,))

        cur.execute(
            """
            INSERT INTO fact_player_match (player_uid, match_uid, points_total, is_home, match_result, raw_json, ingested_at)
            SELECT
                %s, match_uid, points_total, is_home, match_result, raw_json, ingested_at
            FROM fact_player_match
            WHERE player_uid = %s
            ON CONFLICT (player_uid, match_uid)
            DO UPDATE SET
                points_total = EXCLUDED.points_total,
                is_home = COALESCE(EXCLUDED.is_home, fact_player_match.is_home),
                match_result = COALESCE(EXCLUDED.match_result, fact_player_match.match_result),
                raw_json = EXCLUDED.raw_json,
                ingested_at = now()
            """,
            (target_uid, source_uid),
        )
        cur.execute("DELETE FROM fact_player_match WHERE player_uid = %s", (source_uid,))

        cur.execute(
            """
            UPDATE fact_player_event
            SET player_uid = %s,
                ingested_at = now()
            WHERE player_uid = %s
            """,
            (target_uid, source_uid),
        )

        if kb_player_id is not None:
            cur.execute(
                """
                UPDATE dim_player
                SET kb_player_id = %s,
                    updated_at = now()
                WHERE player_uid = %s
                """,
                (kb_player_id, target_uid),
            )

        cur.execute(
            """
            DELETE FROM dim_player
            WHERE player_uid = %s
              AND player_uid <> %s
            """,
            (source_uid, target_uid),
        )

        cur.execute(
            """
            INSERT INTO etl_state (key, value, updated_at)
            SELECT
                REPLACE(key, %s, %s) AS key,
                value,
                now()
            FROM etl_state
            WHERE key LIKE %s
            ON CONFLICT (key)
            DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = now()
            """,
            (source_uid, target_uid, f"%{source_uid}%"),
        )
        cur.execute(
            """
            DELETE FROM etl_state
            WHERE key LIKE %s
            """,
            (f"%{source_uid}%",),
        )

    return True


def upsert_dim_players(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values_with_kickbase_id: list[tuple[Any, ...]] = []
    values_without_kickbase_id: list[tuple[Any, ...]] = []

    for row in rows:
        value = (
            str(row["player_uid"]),
            _to_int_or_none(row.get("kb_player_id")),
            str(row.get("player_name") or f"player_{row['player_uid']}").strip(),
            _to_int_or_none(row.get("ligainsider_player_id")),
            _to_text_or_none(row.get("ligainsider_player_slug")),
            _to_text_or_none(row.get("ligainsider_player_name")),
            _to_text_or_none(row.get("position")),
            row.get("birthdate"),
            row.get("image_blob"),
            _to_text_or_none(row.get("image_mime")),
            _to_text_or_none(row.get("image_sha256")),
            _to_text_or_none(row.get("image_local_path")),
        )
        if value[1] is None:
            values_without_kickbase_id.append(value)
        else:
            values_with_kickbase_id.append(value)

    inserted_total = 0
    updated_total = 0

    sql_with_kickbase_id = """
        INSERT INTO dim_player (
            player_uid,
            kb_player_id,
            player_name,
            ligainsider_player_id,
            ligainsider_player_slug,
            ligainsider_player_name,
            position,
            birthdate,
            image_blob,
            image_mime,
            image_sha256,
            image_local_path
        )
        VALUES %s
        ON CONFLICT (kb_player_id)
        DO UPDATE SET
            player_uid = EXCLUDED.player_uid,
            player_name = EXCLUDED.player_name,
            ligainsider_player_id = COALESCE(EXCLUDED.ligainsider_player_id, dim_player.ligainsider_player_id),
            ligainsider_player_slug = COALESCE(EXCLUDED.ligainsider_player_slug, dim_player.ligainsider_player_slug),
            ligainsider_player_name = COALESCE(EXCLUDED.ligainsider_player_name, dim_player.ligainsider_player_name),
            position = COALESCE(EXCLUDED.position, dim_player.position),
            birthdate = COALESCE(EXCLUDED.birthdate, dim_player.birthdate),
            image_blob = CASE
                WHEN EXCLUDED.image_sha256 IS NOT NULL
                     AND EXCLUDED.image_sha256 IS DISTINCT FROM dim_player.image_sha256
                THEN EXCLUDED.image_blob
                ELSE dim_player.image_blob
            END,
            image_mime = CASE
                WHEN EXCLUDED.image_sha256 IS NOT NULL
                     AND EXCLUDED.image_sha256 IS DISTINCT FROM dim_player.image_sha256
                THEN EXCLUDED.image_mime
                ELSE dim_player.image_mime
            END,
            image_sha256 = COALESCE(EXCLUDED.image_sha256, dim_player.image_sha256),
            image_local_path = COALESCE(EXCLUDED.image_local_path, dim_player.image_local_path),
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    sql_without_kickbase_id = """
        INSERT INTO dim_player (
            player_uid,
            kb_player_id,
            player_name,
            ligainsider_player_id,
            ligainsider_player_slug,
            ligainsider_player_name,
            position,
            birthdate,
            image_blob,
            image_mime,
            image_sha256,
            image_local_path
        )
        VALUES %s
        ON CONFLICT (player_uid)
        DO UPDATE SET
            player_name = EXCLUDED.player_name,
            ligainsider_player_id = COALESCE(EXCLUDED.ligainsider_player_id, dim_player.ligainsider_player_id),
            ligainsider_player_slug = COALESCE(EXCLUDED.ligainsider_player_slug, dim_player.ligainsider_player_slug),
            ligainsider_player_name = COALESCE(EXCLUDED.ligainsider_player_name, dim_player.ligainsider_player_name),
            position = COALESCE(EXCLUDED.position, dim_player.position),
            birthdate = COALESCE(EXCLUDED.birthdate, dim_player.birthdate),
            image_blob = CASE
                WHEN EXCLUDED.image_sha256 IS NOT NULL
                     AND EXCLUDED.image_sha256 IS DISTINCT FROM dim_player.image_sha256
                THEN EXCLUDED.image_blob
                ELSE dim_player.image_blob
            END,
            image_mime = CASE
                WHEN EXCLUDED.image_sha256 IS NOT NULL
                     AND EXCLUDED.image_sha256 IS DISTINCT FROM dim_player.image_sha256
                THEN EXCLUDED.image_mime
                ELSE dim_player.image_mime
            END,
            image_sha256 = COALESCE(EXCLUDED.image_sha256, dim_player.image_sha256),
            image_local_path = COALESCE(EXCLUDED.image_local_path, dim_player.image_local_path),
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        if values_with_kickbase_id:
            result = execute_values(cur, sql_with_kickbase_id, values_with_kickbase_id, fetch=True)
            inserted, updated = _count_inserted_updated(result)
            inserted_total += inserted
            updated_total += updated

        if values_without_kickbase_id:
            result = execute_values(cur, sql_without_kickbase_id, values_without_kickbase_id, fetch=True)
            inserted, updated = _count_inserted_updated(result)
            inserted_total += inserted
            updated_total += updated

    return inserted_total, updated_total


def upsert_dim_event_types(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            int(row["event_type_id"]),
            str(row.get("event_name") or f"event_{row['event_type_id']}").strip(),
            _to_text_or_none(row.get("template")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO dim_event_type
            (event_type_id, event_name, template)
        VALUES %s
        ON CONFLICT (event_type_id)
        DO UPDATE SET
            event_name = EXCLUDED.event_name,
            template = EXCLUDED.template,
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def ensure_season(conn: PgConnection, *, league_key: str, season_label: str) -> int:
    season_uid = _season_uid_from_label(season_label)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_season (season_uid, league_key, season_label)
            VALUES (%s, %s, %s)
            ON CONFLICT (league_key, season_label)
            DO UPDATE SET updated_at = now()
            RETURNING season_uid
            """,
            (season_uid, league_key, season_label),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("Could not resolve season_uid")
    return int(row[0])


def upsert_dim_teams(
    conn: PgConnection,
    *,
    league_key: str,
    rows: Sequence[dict[str, Any]],
) -> tuple[TeamLookup, int, int]:
    inserted_total = 0
    updated_total = 0
    by_kickbase_team_id: dict[int, str] = {}
    by_team_code: dict[str, str] = {}

    if not rows:
        return TeamLookup(by_kickbase_team_id, by_team_code), inserted_total, updated_total

    dedup: dict[tuple[int | None, str | None], dict[str, Any]] = {}
    for row in rows:
        kickbase_team_id = _to_int_or_none(row.get("kickbase_team_id"))
        team_code = _to_text_or_none(row.get("team_code"))
        team_name = _to_text_or_none(row.get("team_name"))
        if kickbase_team_id is None and team_code is None:
            continue
        dedup[(kickbase_team_id, team_code)] = {
            "kickbase_team_id": kickbase_team_id,
            "team_code": team_code,
            "team_uid": _build_team_uid(team_code=team_code, kickbase_team_id=kickbase_team_id),
            "team_name": team_name,
        }

    with conn.cursor() as cur:
        for row in dedup.values():
            kickbase_team_id = row["kickbase_team_id"]
            team_code = row["team_code"]
            team_uid = row["team_uid"]
            team_name = row["team_name"]

            if team_code is not None:
                cur.execute(
                    """
                    INSERT INTO dim_team (team_uid, league_key, kickbase_team_id, team_code, team_name)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (league_key, team_code)
                    DO UPDATE SET
                        team_uid = EXCLUDED.team_uid,
                        kickbase_team_id = COALESCE(EXCLUDED.kickbase_team_id, dim_team.kickbase_team_id),
                        team_name = COALESCE(EXCLUDED.team_name, dim_team.team_name),
                        updated_at = now()
                    RETURNING team_uid, (xmax = 0) AS inserted
                    """,
                    (team_uid, league_key, kickbase_team_id, team_code, team_name),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO dim_team (team_uid, league_key, kickbase_team_id, team_code, team_name)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (league_key, kickbase_team_id)
                    DO UPDATE SET
                        team_uid = EXCLUDED.team_uid,
                        team_name = COALESCE(EXCLUDED.team_name, dim_team.team_name),
                        updated_at = now()
                    RETURNING team_uid, (xmax = 0) AS inserted
                    """,
                    (team_uid, league_key, kickbase_team_id, team_code, team_name),
                )

            team_uid, inserted_flag = cur.fetchone()
            inserted_total += 1 if bool(inserted_flag) else 0
            updated_total += 0 if bool(inserted_flag) else 1
            team_uid_text = str(team_uid)
            if kickbase_team_id is not None:
                by_kickbase_team_id[int(kickbase_team_id)] = team_uid_text
            if team_code is not None:
                by_team_code[str(team_code).upper()] = team_uid_text

        if by_kickbase_team_id:
            cur.execute(
                """
                SELECT kickbase_team_id, team_uid
                FROM dim_team
                WHERE league_key = %s
                  AND kickbase_team_id = ANY(%s)
                """,
                (league_key, list(by_kickbase_team_id.keys())),
            )
            for kickbase_team_id, team_uid in cur.fetchall():
                if kickbase_team_id is not None:
                    by_kickbase_team_id[int(kickbase_team_id)] = str(team_uid)

        if by_team_code:
            cur.execute(
                """
                SELECT team_code, team_uid
                FROM dim_team
                WHERE league_key = %s
                  AND team_code = ANY(%s)
                """,
                (league_key, list(by_team_code.keys())),
            )
            for team_code, team_uid in cur.fetchall():
                if team_code:
                    by_team_code[str(team_code).upper()] = str(team_uid)

    return TeamLookup(by_kickbase_team_id, by_team_code), inserted_total, updated_total


def upsert_bridge_player_team(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            str(row["player_uid"]),
            int(row["season_uid"]),
            str(row["team_uid"]),
            row.get("valid_from"),
            row.get("valid_to"),
            str(row.get("source") or "kickbase"),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO bridge_player_team (
            player_uid,
            season_uid,
            team_uid,
            valid_from,
            valid_to,
            source
        )
        VALUES %s
        ON CONFLICT (player_uid, season_uid, source)
        DO UPDATE SET
            team_uid = EXCLUDED.team_uid,
            valid_from = COALESCE(EXCLUDED.valid_from, bridge_player_team.valid_from),
            valid_to = COALESCE(EXCLUDED.valid_to, bridge_player_team.valid_to),
            updated_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def upsert_dim_matches(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            str(row["match_uid"]),
            _to_int_or_none(row.get("kickbase_match_id")),
            str(row["league_key"]),
            _to_int_or_none(row.get("season_uid")),
            str(row["season_label"]),
            int(row["matchday"]),
            _to_text_or_none(row.get("home_team_uid")),
            _to_text_or_none(row.get("away_team_uid")),
            row.get("kickoff_ts"),
            _to_int_or_none(row.get("score_home")),
            _to_int_or_none(row.get("score_away")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO dim_match (
            match_uid,
            kickbase_match_id,
            league_key,
            season_uid,
            season_label,
            matchday,
            home_team_uid,
            away_team_uid,
            kickoff_ts,
            score_home,
            score_away
        )
        VALUES %s
        ON CONFLICT (match_uid)
        DO UPDATE SET
            kickbase_match_id = COALESCE(EXCLUDED.kickbase_match_id, dim_match.kickbase_match_id),
            league_key = EXCLUDED.league_key,
            season_uid = COALESCE(EXCLUDED.season_uid, dim_match.season_uid),
            season_label = EXCLUDED.season_label,
            matchday = EXCLUDED.matchday,
            home_team_uid = COALESCE(EXCLUDED.home_team_uid, dim_match.home_team_uid),
            away_team_uid = COALESCE(EXCLUDED.away_team_uid, dim_match.away_team_uid),
            kickoff_ts = COALESCE(EXCLUDED.kickoff_ts, dim_match.kickoff_ts),
            score_home = COALESCE(EXCLUDED.score_home, dim_match.score_home),
            score_away = COALESCE(EXCLUDED.score_away, dim_match.score_away),
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
            FROM fact_market_value_daily
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
            row["mv_date"],
            int(row["market_value"]),
            _to_int_or_none(row.get("source_dt_days")),
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_market_value_daily
            (player_uid, mv_date, market_value, source_dt_days)
        VALUES %s
        ON CONFLICT (player_uid, mv_date)
        DO UPDATE SET
            market_value = EXCLUDED.market_value,
            source_dt_days = COALESCE(EXCLUDED.source_dt_days, fact_market_value_daily.source_dt_days),
            ingested_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def upsert_fact_player_match(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    values = [
        (
            str(row["player_uid"]),
            str(row["match_uid"]),
            int(row["points_total"]),
            _to_bool_or_none(row.get("is_home")),
            _to_text_or_none(row.get("match_result")),
            Json(row.get("raw_json")) if row.get("raw_json") is not None else None,
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_player_match (
            player_uid,
            match_uid,
            points_total,
            is_home,
            match_result,
            raw_json
        )
        VALUES %s
        ON CONFLICT (player_uid, match_uid)
        DO UPDATE SET
            points_total = EXCLUDED.points_total,
            is_home = COALESCE(EXCLUDED.is_home, fact_player_match.is_home),
            match_result = COALESCE(EXCLUDED.match_result, fact_player_match.match_result),
            raw_json = EXCLUDED.raw_json,
            ingested_at = now()
        RETURNING (xmax = 0) AS inserted
    """

    with conn.cursor() as cur:
        result = execute_values(cur, sql, values, fetch=True)
    return _count_inserted_updated(result)


def insert_fact_player_events(conn: PgConnection, rows: Sequence[dict[str, Any]]) -> int:
    if not rows:
        return 0

    values = [
        (
            str(row["event_hash"]),
            str(row["player_uid"]),
            str(row["match_uid"]),
            int(row["event_type_id"]),
            int(row["points"]),
            _to_int_or_none(row.get("mt")),
            _to_text_or_none(row.get("att")),
            Json(row.get("raw_event")) if row.get("raw_event") is not None else None,
        )
        for row in rows
    ]

    sql = """
        INSERT INTO fact_player_event (
            event_hash,
            player_uid,
            match_uid,
            event_type_id,
            points,
            mt,
            att,
            raw_event
        )
        VALUES %s
        ON CONFLICT (event_hash) DO NOTHING
        RETURNING 1
    """

    with conn.cursor() as cur:
        inserted = execute_values(cur, sql, values, fetch=True)
    return len(inserted)


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


def export_raw_tables_to_csv(conn: PgConnection, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    table_selects: list[tuple[str, str]] = [
        (
            "dim_player",
            """
            SELECT
                player_uid,
                kb_player_id,
                player_name,
                ligainsider_player_id,
                ligainsider_player_slug,
                ligainsider_player_name,
                position,
                birthdate,
                image_mime,
                image_sha256,
                image_local_path,
                created_at,
                updated_at
            FROM dim_player
            ORDER BY player_uid
            """,
        ),
        ("dim_season", "SELECT * FROM dim_season ORDER BY season_uid"),
        ("dim_team", "SELECT * FROM dim_team ORDER BY team_uid"),
        (
            "bridge_player_team",
            "SELECT * FROM bridge_player_team ORDER BY season_uid, team_uid, player_uid",
        ),
        ("dim_match", "SELECT * FROM dim_match ORDER BY season_label DESC, matchday DESC, match_uid"),
        ("dim_event_type", "SELECT * FROM dim_event_type ORDER BY event_type_id"),
        (
            "fact_market_value_daily",
            "SELECT * FROM fact_market_value_daily ORDER BY mv_date DESC, player_uid",
        ),
        (
            "fact_player_match",
            "SELECT * FROM fact_player_match ORDER BY ingested_at DESC, player_uid",
        ),
        (
            "fact_player_event",
            "SELECT * FROM fact_player_event ORDER BY ingested_at DESC, player_uid",
        ),
        ("etl_state", "SELECT * FROM etl_state ORDER BY key"),
    ]

    written: list[Path] = []
    with conn.cursor() as cur:
        for table_name, query in table_selects:
            cur.execute(query)
            rows = cur.fetchall()
            headers = [col.name for col in cur.description]
            out_path = output_dir / f"{table_name}.csv"
            with out_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                writer.writerows(rows)
            written.append(out_path)

    return written


def export_player_images(conn: PgConnection, output_dir: Path) -> tuple[list[Path], Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    mapping_path = output_dir / "image_mapping.csv"
    written_images: list[Path] = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_uid, image_mime, image_sha256, image_blob
            FROM dim_player
            WHERE image_blob IS NOT NULL
            ORDER BY player_uid
            """
        )
        rows = cur.fetchall()

    with mapping_path.open("w", encoding="utf-8", newline="") as mapping_handle:
        writer = csv.writer(mapping_handle)
        writer.writerow(["player_uid", "image_mime", "image_sha256", "filename"])

        for player_uid, image_mime, image_sha256, image_blob in rows:
            if image_blob is None:
                continue
            extension = _extension_from_mime(_to_text_or_none(image_mime))
            filename = f"{_safe_filename(str(player_uid))}.{extension}"
            image_path = output_dir / filename
            image_path.write_bytes(bytes(image_blob))
            written_images.append(image_path)
            writer.writerow([str(player_uid), image_mime, image_sha256, filename])

    return written_images, mapping_path


def _extension_from_mime(mime: str | None) -> str:
    if not mime:
        return "bin"
    lowered = mime.lower().strip()
    if lowered == "image/jpeg":
        return "jpg"
    if lowered == "image/png":
        return "png"
    return "bin"


def _count_inserted_updated(result: Sequence[Sequence[Any]]) -> tuple[int, int]:
    inserted = 0
    total = 0
    for row in result:
        if not row:
            continue
        total += 1
        if bool(row[0]):
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


def _safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return cleaned or "player"


def _build_team_uid(*, team_code: str | None, kickbase_team_id: int | None) -> str:
    normalized_code = _to_text_or_none(team_code)
    if normalized_code:
        return normalized_code.upper()
    if kickbase_team_id is not None:
        return f"KB{kickbase_team_id}"
    return "KBNA"


def _season_uid_from_label(season_label: str) -> int:
    text = (season_label or "").strip()
    match = re.match(r"^(\d{4})\s*/\s*(\d{4})$", text)
    if match is not None:
        return int(match.group(1)[-2:] + match.group(2)[-2:])

    # deterministic fallback for non-standard labels
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return 900000 + (int(digest[:6], 16) % 9999)
