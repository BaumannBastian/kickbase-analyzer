# ------------------------------------
# etl_history.py
#
# ETL-Pipeline fuer Kickbase-History nach PostgreSQL.
# Laedt Spielerliste aus Databricks oder CSV und schreibt
# Marktwert-Historie, Match-Performance und Event-Breakdowns
# idempotent/inkrementell in Postgres.
#
# Usage
# ------------------------------------
# - python -m src.etl_history --players-csv ./in/players.csv --max-players 1
# - python -m src.etl_history --max-players 5 --days-from 1 --days-to 3
# ------------------------------------

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


LOGGER = logging.getLogger("kickbase_history_etl")


@dataclass(frozen=True)
class PlayerMaster:
    player_id: int
    name: str
    team_id: int | None
    team_name: str | None
    position: str | None
    competition_id: int | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kickbase history ETL -> PostgreSQL")
    parser.add_argument("--env-file", default=".env", help="Pfad zur .env Datei")
    parser.add_argument("--players-csv", default=None, help="Fallback CSV mit player_id,name,...")
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--competition-name", default="Bundesliga")
    parser.add_argument("--league-id", default=os.getenv("KICKBASE_LEAGUE_ID", ""))
    parser.add_argument("--max-players", type=int, default=None)
    parser.add_argument("--player-id", type=int, action="append", default=[])
    parser.add_argument("--player-name-like", default=None)
    parser.add_argument("--rps", type=float, default=3.0)
    parser.add_argument("--timeframe-days", type=int, default=3650)
    parser.add_argument("--days-from", type=int, default=1)
    parser.add_argument("--days-to", type=int, default=None)
    parser.add_argument("--season-label", default=None, help="Override fuer Event-Saison, z.B. 2025/2026")
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--raw-dir", default="out/raw")
    parser.add_argument("--databricks-query", default=None)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def load_players(args: argparse.Namespace) -> list[PlayerMaster]:
    if args.players_csv:
        return _load_players_from_csv(Path(args.players_csv))

    table = os.getenv("DATABRICKS_PLAYERS_TABLE", "").strip()
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME", "").strip()
    path = os.getenv("DATABRICKS_HTTP_PATH", "").strip()
    token = os.getenv("DATABRICKS_TOKEN", "").strip()

    if not (table and host and path and token):
        raise RuntimeError(
            "Spielerliste konnte nicht geladen werden: Databricks ENV fehlen und --players-csv wurde nicht gesetzt."
        )

    query = args.databricks_query or _default_databricks_query(table)
    return _load_players_from_databricks(query)


def _default_databricks_query(table: str) -> str:
    return f"""
    SELECT DISTINCT
        CAST(kickbase_player_id AS BIGINT) AS player_id,
        player_name AS name,
        CAST(team_id AS BIGINT) AS team_id,
        CAST(NULL AS STRING) AS team_name,
        position,
        CAST(NULL AS BIGINT) AS competition_id
    FROM {table}
    WHERE kickbase_player_id IS NOT NULL
    """


def _load_players_from_databricks(query: str) -> list[PlayerMaster]:
    try:
        from databricks import sql as databricks_sql
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector fehlt. Bitte zuerst requirements installieren."
        ) from exc

    host = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    token = os.environ["DATABRICKS_TOKEN"]

    rows: list[PlayerMaster] = []
    with databricks_sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [str(desc[0]).lower() for desc in cur.description]
            for raw in cur.fetchall():
                row = dict(zip(cols, raw, strict=False))
                player = _player_from_mapping(row)
                if player is not None:
                    rows.append(player)

    if not rows:
        raise RuntimeError("Databricks-Query lieferte keine Spieler")
    return rows


def _load_players_from_csv(path: Path) -> list[PlayerMaster]:
    if not path.exists():
        raise FileNotFoundError(f"CSV nicht gefunden: {path}")

    rows: list[PlayerMaster] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for item in reader:
            player = _player_from_mapping(item)
            if player is not None:
                rows.append(player)

    if not rows:
        raise RuntimeError(f"CSV enthaelt keine gueltigen Spieler: {path}")
    return rows


def _player_from_mapping(row: dict[str, Any]) -> PlayerMaster | None:
    player_id = _to_int(_first_present(row, ["player_id", "kickbase_player_id", "id", "i", "pi"]))
    if player_id is None:
        return None

    name = _to_text(_first_present(row, ["name", "player_name", "n"])) or f"player_{player_id}"

    return PlayerMaster(
        player_id=player_id,
        name=name,
        team_id=_to_int(_first_present(row, ["team_id", "tid", "t" ])),
        team_name=_to_text_or_none(_first_present(row, ["team_name", "team"])) ,
        position=_to_text_or_none(_first_present(row, ["position", "pos"])),
        competition_id=_to_int(_first_present(row, ["competition_id", "competitionid", "cpi"])),
    )


def select_players(players: list[PlayerMaster], args: argparse.Namespace) -> list[PlayerMaster]:
    out = players

    if args.player_id:
        wanted = set(args.player_id)
        out = [player for player in out if player.player_id in wanted]

    if args.player_name_like:
        needle = args.player_name_like.strip().lower()
        out = [player for player in out if needle in player.name.lower()]

    out = sorted(out, key=lambda player: (player.name.lower(), player.player_id))

    if args.max_players is not None and args.max_players > 0:
        out = out[: args.max_players]

    if not out:
        raise RuntimeError("Nach Filtern sind keine Spieler uebrig")
    return out


def extract_rows(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if isinstance(payload, dict):
        for key in ("data", "items", "rows", "it", "result", "players", "history", "ph"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [row for row in candidate if isinstance(row, dict)]
    return []


def detect_current_matchday(payload: dict[str, Any] | list[Any]) -> int | None:
    rows = extract_rows(payload)
    current_day: int | None = None
    max_day: int | None = None

    for row in rows:
        day = _to_int(_first_present(row, ["dayNumber", "matchday", "day", "id", "n"]))
        if day is None:
            continue

        if max_day is None or day > max_day:
            max_day = day

        is_current = _to_bool(_first_present(row, ["isCurrent", "current", "cur"]))
        if is_current:
            current_day = day

    return current_day if current_day is not None else max_day


def detect_season_label(payload: dict[str, Any] | list[Any]) -> str | None:
    rows = extract_rows(payload)
    for row in rows:
        season = _to_text(_first_present(row, ["ti", "season", "season_label", "sn", "seasonLabel"]))
        if season:
            return season
    return None


def default_season_label(now: datetime | None = None) -> str:
    ref = now or datetime.now(UTC)
    start_year = ref.year if ref.month >= 7 else ref.year - 1
    return f"{start_year}/{start_year + 1}"


def parse_event_types(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    direct_rows = extract_rows(payload)
    for row in direct_rows:
        event_type_id = _to_int(
            _first_present(row, ["eti", "eventTypeId", "event_type_id", "id", "i"])
        )
        if event_type_id is None:
            continue
        rows.append(
            {
                "event_type_id": event_type_id,
                "name": _to_text(_first_present(row, ["name", "n", "title", "ti"]))
                or f"eti_{event_type_id}",
                "template": _to_text_or_none(_first_present(row, ["template", "tpl", "att", "te"])),
            }
        )

    if rows:
        return _dedupe_by_key(rows, "event_type_id")

    if isinstance(payload, dict):
        for maybe_key, value in payload.items():
            event_type_id = _to_int(maybe_key)
            if event_type_id is None:
                continue
            if isinstance(value, str):
                rows.append({"event_type_id": event_type_id, "name": value, "template": None})
            elif isinstance(value, dict):
                rows.append(
                    {
                        "event_type_id": event_type_id,
                        "name": _to_text(_first_present(value, ["name", "n", "title", "ti"]))
                        or f"eti_{event_type_id}",
                        "template": _to_text_or_none(
                            _first_present(value, ["template", "tpl", "att", "te"])
                        ),
                    }
                )

    return _dedupe_by_key(rows, "event_type_id")


def parse_market_value_history(
    payload: dict[str, Any] | list[Any],
    *,
    player_id: int,
) -> list[dict[str, Any]]:
    rows = extract_rows(payload)
    out: list[dict[str, Any]] = []

    for row in rows:
        dt = _parse_datetime(_first_present(row, ["d", "dt", "date", "timestamp", "ts", "t"]))
        if dt is None:
            continue

        market_value = _to_int(_first_present(row, ["mv", "market_value", "value", "v"]))
        if market_value is None:
            continue

        out.append(
            {
                "player_id": player_id,
                "mv_date": dt.date(),
                "market_value": market_value,
                "source_dt_days": _to_int(row.get("dt")) if isinstance(row, dict) else None,
            }
        )

    out.sort(key=lambda item: item["mv_date"])

    dedup: dict[date, dict[str, Any]] = {}
    for row in out:
        dedup[row["mv_date"]] = row
    return [dedup[key] for key in sorted(dedup.keys())]


def parse_performance_rows(
    payload: dict[str, Any] | list[Any],
    *,
    player_id: int,
    competition_id: int,
) -> list[dict[str, Any]]:
    rows = _extract_performance_entries(payload)
    out: list[dict[str, Any]] = []

    for row, season_label in rows:
        matchday = _to_int(_first_present(row, ["matchday", "dayNumber", "spieltag", "day"]))
        points_total = _to_int(_first_present(row, ["p", "points", "pts", "total_points"]))
        if matchday is None or points_total is None:
            continue

        match_ts = _parse_datetime(
            _first_present(row, ["match_ts", "kickoff", "d", "date", "ts", "md"])
        )
        score_home = _to_int(_first_present(row, ["scoreHome", "score_home", "sh", "t1g"]))
        score_away = _to_int(_first_present(row, ["scoreAway", "score_away", "sa", "t2g"]))

        if score_home is None or score_away is None:
            score_text = _to_text(_first_present(row, ["score", "result", "res"]))
            parsed = _parse_score(score_text)
            if parsed is not None:
                score_home, score_away = parsed

        team_id = _to_int(_first_present(row, ["team_id", "tid", "t", "pt"]))
        t1 = _to_int(_first_present(row, ["t1", "team_home_id"]))
        t2 = _to_int(_first_present(row, ["t2", "team_away_id"]))
        opponent_team_id = _to_int(
            _first_present(row, ["opponent_team_id", "opp_tid", "otid", "opponentTeamId"])
        )
        if opponent_team_id is None and team_id is not None:
            if t1 is not None and t2 is not None:
                if t1 == team_id:
                    opponent_team_id = t2
                elif t2 == team_id:
                    opponent_team_id = t1

        is_home: bool | None = None
        if team_id is not None and t1 is not None and t2 is not None:
            if team_id == t1:
                is_home = True
            elif team_id == t2:
                is_home = False

        match_result: str | None = None
        if score_home is not None and score_away is not None and is_home is not None:
            own_score = score_home if is_home else score_away
            opp_score = score_away if is_home else score_home
            if own_score > opp_score:
                match_result = "W"
            elif own_score < opp_score:
                match_result = "L"
            else:
                match_result = "D"

        out.append(
            {
                "player_id": player_id,
                "competition_id": competition_id,
                "season_label": season_label or "unknown",
                "matchday": matchday,
                "match_id": _to_int(_first_present(row, ["match_id", "matchId", "mid", "m" ])),
                "match_ts": match_ts,
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "opponent_name": _to_text_or_none(
                    _first_present(row, ["opponent_name", "opp", "opponent"])
                ),
                "is_home": is_home,
                "score_home": score_home,
                "score_away": score_away,
                "match_result": match_result,
                "points_total": points_total,
                "raw_json": row,
            }
        )

    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for row in out:
        dedup[(str(row["season_label"]), int(row["matchday"]))] = row
    return [dedup[key] for key in sorted(dedup.keys())]


def _extract_performance_entries(payload: dict[str, Any] | list[Any]) -> list[tuple[dict[str, Any], str]]:
    entries: list[tuple[dict[str, Any], str]] = []

    def _walk(node: Any, season: str) -> None:
        if isinstance(node, dict):
            local_season = season
            node_season = _to_text(_first_present(node, ["ti", "season", "season_label", "sn"]))
            if node_season:
                local_season = node_season

            looks_like_match = (
                _first_present(node, ["day", "matchday", "md", "dayNumber"]) is not None
                and _first_present(node, ["p", "points", "pts", "total_points"]) is not None
            )
            if looks_like_match:
                entries.append((node, local_season or "unknown"))

            for value in node.values():
                if isinstance(value, (dict, list)):
                    _walk(value, local_season)

        elif isinstance(node, list):
            for item in node:
                _walk(item, season)

    _walk(payload, "unknown")
    return entries


def parse_playercenter_events(
    payload: dict[str, Any] | list[Any],
    *,
    player_id: int,
    competition_id: int,
    season_label: str,
    matchday: int,
) -> list[dict[str, Any]]:
    event_dicts = _find_event_dicts(payload)
    rows: list[dict[str, Any]] = []

    for event in event_dicts:
        event_type_id = _to_int(_first_present(event, ["eti", "eventTypeId", "event_type_id", "id"]))
        points = _to_int(_first_present(event, ["p", "points", "pts", "value"]))
        if event_type_id is None or points is None:
            continue

        mt = _to_int(_first_present(event, ["mt", "minute", "min"]))
        att = _to_text_or_none(_first_present(event, ["att", "text", "label", "template"]))
        dedup_key = _build_event_dedup_key(
            player_id=player_id,
            competition_id=competition_id,
            season_label=season_label,
            matchday=matchday,
            event_type_id=event_type_id,
            points=points,
            mt=mt,
            att=att,
        )

        rows.append(
            {
                "player_id": player_id,
                "competition_id": competition_id,
                "season_label": season_label,
                "matchday": matchday,
                "event_type_id": event_type_id,
                "points": points,
                "mt": mt,
                "att": att,
                "event_dedup_key": dedup_key,
                "raw_event": event,
            }
        )

    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        unique[row["event_dedup_key"]] = row
    return list(unique.values())


def _find_event_dicts(payload: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            has_type = any(key in node for key in ("eti", "eventTypeId", "event_type_id"))
            has_points = any(key in node for key in ("p", "points", "pts", "value"))
            if has_type and has_points:
                out.append(node)
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(payload)
    return out


def _build_event_dedup_key(
    *,
    player_id: int,
    competition_id: int,
    season_label: str,
    matchday: int,
    event_type_id: int,
    points: int,
    mt: int | None,
    att: str | None,
) -> str:
    raw = "|".join(
        [
            str(player_id),
            str(competition_id),
            str(season_label),
            str(matchday),
            str(event_type_id),
            str(points),
            "" if mt is None else str(mt),
            "" if att is None else att,
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _parse_score(text: str) -> tuple[int, int] | None:
    cleaned = text.replace(" ", "")
    if ":" not in cleaned:
        return None
    left, right = cleaned.split(":", 1)
    try:
        return int(left), int(right)
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)

    if isinstance(value, (int, float)):
        ts = float(value)
        # Kickbase `dt` in Market-Value-History ist Tage-seit-Epoch (z.B. 20505).
        if 10_000 <= ts <= 100_000:
            return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=ts)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _first_present(row: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _to_text_or_none(value: Any) -> str | None:
    text = _to_text(value)
    return text or None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _dedupe_by_key(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    dedup: dict[Any, dict[str, Any]] = {}
    for row in rows:
        dedup[row[key]] = row
    return [dedup[item] for item in sorted(dedup.keys())]


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging(args.log_level)

    try:
        from src.db import (
            DbConfig,
            cleanup_unknown_event_rows_for_player,
            get_connection,
            get_max_market_value_date,
            refresh_match_event_agg,
            set_state,
            upsert_event_types,
            upsert_market_values,
            upsert_match_performance,
            upsert_players,
            insert_match_events,
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg2 fehlt. Bitte zuerst `python -m pip install -r requirements.txt` ausfuehren."
        ) from exc

    from src.kickbase_client import KickbaseApiConfig, KickbaseClient

    players = load_players(args)
    players = select_players(players, args)

    kb_config = KickbaseApiConfig.from_env(rps=args.rps)
    if not kb_config.email or not kb_config.password:
        raise RuntimeError("KICKBASE_EMAIL/KICKBASE_PASSWORD sind nicht gesetzt")

    db_config = DbConfig.from_env()

    raw_root = Path(args.raw_dir)

    summary: dict[str, Any] = {
        "players_processed": 0,
        "marketvalue_inserted": 0,
        "marketvalue_updated": 0,
        "performance_inserted": 0,
        "performance_updated": 0,
        "match_events_inserted": 0,
        "match_events_unknown_cleanup_deleted": 0,
        "match_event_agg_unknown_cleanup_deleted": 0,
        "event_types_inserted": 0,
        "event_types_updated": 0,
        "earliest_marketvalue_date": None,
        "latest_marketvalue_date": None,
    }

    client = KickbaseClient(kb_config)
    token = client.authenticate()

    competition_id = args.competition_id
    if competition_id is None:
        explicit = sorted({player.competition_id for player in players if player.competition_id is not None})
        if len(explicit) == 1:
            competition_id = explicit[0]

    if competition_id is None:
        competition_id = client.discover_competition_id(
            token=token,
            league_id=args.league_id,
            competition_name=args.competition_name,
        )

    if competition_id is None:
        raise RuntimeError(
            "Competition ID konnte nicht ermittelt werden. Bitte --competition-id explizit setzen."
        )

    matchdays_payload = client.get_matchdays(token, competition_id)
    current_day = detect_current_matchday(matchdays_payload)
    if current_day is None:
        raise RuntimeError("Matchday-Range konnte nicht aus /matchdays gelesen werden")

    days_to = args.days_to if args.days_to is not None else current_day
    if days_to < args.days_from:
        raise RuntimeError("--days-to darf nicht kleiner als --days-from sein")
    season_label = args.season_label or detect_season_label(matchdays_payload) or default_season_label()
    if str(season_label).strip().lower() in {"", "unknown"}:
        season_label = default_season_label()

    event_types_payload = client.get_event_types(token)
    event_types = parse_event_types(event_types_payload)

    with get_connection(db_config) as conn:
        inserted, updated = upsert_event_types(conn, event_types)
        summary["event_types_inserted"] += inserted
        summary["event_types_updated"] += updated
        set_state(conn, "last_eventtypes_sync_ts", datetime.now(UTC).isoformat().replace("+00:00", "Z"))
        conn.commit()

        for idx, player in enumerate(players, start=1):
            player_competition_id = player.competition_id or competition_id
            if player_competition_id is None:
                LOGGER.warning("Skip player %s: no competition id", player.player_id)
                continue

            LOGGER.info(
                "[%s/%s] player=%s (%s) competition=%s",
                idx,
                len(players),
                player.name,
                player.player_id,
                player_competition_id,
            )

            upsert_players(
                conn,
                [
                    {
                        "player_id": player.player_id,
                        "name": player.name,
                        "team_id": player.team_id,
                        "team_name": player.team_name,
                        "position": player.position,
                        "competition_id": player_competition_id,
                    }
                ],
            )

            market_payload = client.get_market_value_history(
                token,
                player_competition_id,
                player.player_id,
                args.timeframe_days,
            )
            if args.save_raw:
                _write_json(
                    raw_root
                    / str(player.player_id)
                    / f"marketvalue_{player_competition_id}_{args.timeframe_days}.json",
                    market_payload,
                )

            market_rows = parse_market_value_history(market_payload, player_id=player.player_id)
            max_existing_date = get_max_market_value_date(conn, player.player_id)
            if max_existing_date is not None:
                market_rows = [row for row in market_rows if row["mv_date"] > max_existing_date]

            inserted_mv, updated_mv = upsert_market_values(conn, market_rows)
            summary["marketvalue_inserted"] += inserted_mv
            summary["marketvalue_updated"] += updated_mv

            for row in market_rows:
                mv_date = row["mv_date"]
                earliest = summary["earliest_marketvalue_date"]
                latest = summary["latest_marketvalue_date"]
                if earliest is None or mv_date < earliest:
                    summary["earliest_marketvalue_date"] = mv_date
                if latest is None or mv_date > latest:
                    summary["latest_marketvalue_date"] = mv_date

            performance_payload = client.get_performance(
                token,
                player_competition_id,
                player.player_id,
            )
            if args.save_raw:
                _write_json(
                    raw_root / str(player.player_id) / f"performance_{player_competition_id}.json",
                    performance_payload,
                )

            perf_rows = parse_performance_rows(
                performance_payload,
                player_id=player.player_id,
                competition_id=player_competition_id,
            )
            inserted_perf, updated_perf = upsert_match_performance(conn, perf_rows)
            summary["performance_inserted"] += inserted_perf
            summary["performance_updated"] += updated_perf

            touched_days: set[int] = set()
            known_event_type_ids = {int(row["event_type_id"]) for row in event_types}

            for day_number in range(args.days_from, days_to + 1):
                playercenter_payload = client.get_playercenter(
                    token,
                    player_competition_id,
                    player.player_id,
                    day_number,
                )
                if args.save_raw:
                    _write_json(
                        raw_root
                        / str(player.player_id)
                        / f"playercenter_{player_competition_id}_day_{day_number}.json",
                        playercenter_payload,
                    )

                event_rows = parse_playercenter_events(
                    playercenter_payload,
                    player_id=player.player_id,
                    competition_id=player_competition_id,
                    season_label=season_label,
                    matchday=day_number,
                )
                if event_rows:
                    missing_event_type_ids = sorted(
                        {int(row["event_type_id"]) for row in event_rows} - known_event_type_ids
                    )
                    if missing_event_type_ids:
                        dynamic_event_types = [
                            {
                                "event_type_id": int(event_type_id),
                                "name": f"event_{event_type_id}",
                                "template": None,
                            }
                            for event_type_id in missing_event_type_ids
                        ]
                        inserted_dyn, updated_dyn = upsert_event_types(conn, dynamic_event_types)
                        summary["event_types_inserted"] += inserted_dyn
                        summary["event_types_updated"] += updated_dyn
                        known_event_type_ids.update(missing_event_type_ids)

                inserted_events = insert_match_events(conn, event_rows)
                summary["match_events_inserted"] += inserted_events
                if event_rows:
                    touched_days.add(day_number)

            refresh_match_event_agg(
                conn,
                player_id=player.player_id,
                competition_id=player_competition_id,
                season_label=season_label,
                matchdays=touched_days,
            )
            deleted_events, deleted_agg = cleanup_unknown_event_rows_for_player(
                conn,
                player_id=player.player_id,
                competition_id=player_competition_id,
                season_label=season_label,
            )
            summary["match_events_unknown_cleanup_deleted"] += deleted_events
            summary["match_event_agg_unknown_cleanup_deleted"] += deleted_agg

            set_state(
                conn,
                f"last_marketvalue_date_{player.player_id}",
                str(summary["latest_marketvalue_date"] or ""),
            )
            set_state(
                conn,
                f"last_matchday_processed_{player_competition_id}_{season_label}_{player.player_id}",
                str(days_to),
            )

            conn.commit()
            summary["players_processed"] += 1

    if isinstance(summary["earliest_marketvalue_date"], date):
        summary["earliest_marketvalue_date"] = summary["earliest_marketvalue_date"].isoformat()
    if isinstance(summary["latest_marketvalue_date"], date):
        summary["latest_marketvalue_date"] = summary["latest_marketvalue_date"].isoformat()

    print(json.dumps(summary, ensure_ascii=True, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
