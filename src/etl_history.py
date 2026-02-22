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
import re
from typing import Any, Iterable
import unicodedata
from urllib.parse import urljoin

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


LOGGER = logging.getLogger("kickbase_history_etl")

DEFAULT_TEAM_NAME_BY_CODE = {
    "B04": "Bayer Leverkusen",
    "BMG": "Bor. M'gladbach",
    "BVB": "Borussia Dortmund",
    "FCA": "FC Augsburg",
    "FCB": "FC Bayern",
    "FCH": "1. FC Heidenheim",
    "FCU": "Union Berlin",
    "HSV": "Hamburger SV",
    "KOE": "1. FC Koeln",
    "M05": "Mainz 05",
    "RBL": "RB Leipzig",
    "SCF": "SC Freiburg",
    "SGE": "Eintracht Frankfurt",
    "STP": "FC St. Pauli",
    "SVW": "Werder Bremen",
    "TSG": "TSG Hoffenheim",
    "VFB": "VfB Stuttgart",
    "WOB": "VfL Wolfsburg",
}


@dataclass(frozen=True)
class PlayerMaster:
    kb_player_id: int
    name: str
    team_id: int | None
    team_name: str | None
    position: str | None
    competition_id: int | None
    ligainsider_player_slug: str | None
    ligainsider_player_id: int | None
    birth_date: date | None
    player_image_url: str | None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kickbase history ETL -> PostgreSQL")
    parser.add_argument("--env-file", default=".env", help="Pfad zur .env Datei")
    parser.add_argument("--players-csv", default=None, help="Fallback CSV mit kb_player_id,name,...")
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--competition-name", default="Bundesliga")
    parser.add_argument("--league-id", default=os.getenv("KICKBASE_LEAGUE_ID", ""))
    parser.add_argument("--max-players", type=int, default=None)
    parser.add_argument(
        "--player-id",
        type=int,
        action="append",
        default=[],
        help="Filter auf Kickbase-Spieler-ID (kb_player_id), mehrfach nutzbar",
    )
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
        CAST(kickbase_player_id AS BIGINT) AS kb_player_id,
        player_name AS name,
        CAST(team_id AS BIGINT) AS team_id,
        CAST(NULL AS STRING) AS team_name,
        position,
        CAST(NULL AS BIGINT) AS competition_id,
        CAST(NULL AS STRING) AS ligainsider_player_slug,
        CAST(NULL AS BIGINT) AS ligainsider_player_id,
        CAST(NULL AS DATE) AS birth_date,
        CAST(NULL AS STRING) AS player_image_url
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
    kb_player_id = _to_int(_first_present(row, ["kb_player_id", "player_id", "kickbase_player_id", "id", "i", "pi"]))
    if kb_player_id is None:
        return None

    name = _to_text(_first_present(row, ["name", "player_name", "n"])) or f"player_{kb_player_id}"

    return PlayerMaster(
        kb_player_id=kb_player_id,
        name=name,
        team_id=_to_int(_first_present(row, ["team_id", "tid", "t" ])),
        team_name=_to_text_or_none(_first_present(row, ["team_name", "team"])) ,
        position=_to_text_or_none(_first_present(row, ["position", "pos"])),
        competition_id=_to_int(_first_present(row, ["competition_id", "competitionid", "cpi"])),
        ligainsider_player_slug=_to_text_or_none(
            _first_present(row, ["ligainsider_player_slug", "li_player_slug", "ligainsider_slug"])
        ),
        ligainsider_player_id=_to_int(
            _first_present(row, ["ligainsider_player_id", "li_player_id", "ligainsider_id"])
        ),
        birth_date=_parse_date(_first_present(row, ["birth_date", "birthday", "dob"])),
        player_image_url=_to_text_or_none(
            _first_present(row, ["player_image_url", "ligainsider_image_url", "image_url", "image"])
        ),
    )


def select_players(players: list[PlayerMaster], args: argparse.Namespace) -> list[PlayerMaster]:
    out = players

    if args.player_id:
        wanted = set(args.player_id)
        out = [player for player in out if player.kb_player_id in wanted]

    if args.player_name_like:
        needle = args.player_name_like.strip().lower()
        out = [player for player in out if needle in player.name.lower()]

    out = sorted(out, key=lambda player: (player.name.lower(), player.kb_player_id))

    if args.max_players is not None and args.max_players > 0:
        out = out[: args.max_players]

    if not out:
        raise RuntimeError("Nach Filtern sind keine Spieler uebrig")
    return out


def load_latest_ligainsider_name_map(input_dir: Path) -> dict[str, dict[str, Any]]:
    files = sorted(input_dir.glob("ligainsider_status_snapshot_*.ndjson"))
    if not files:
        return {}

    latest = max(files, key=lambda path: path.name)
    mapping: dict[str, dict[str, Any]] = {}
    collisions: set[str] = set()
    with latest.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            name = _to_text(_first_present(row, ["player_name", "name"]))
            if not name:
                continue
            key = _normalize_name(name)
            if not key:
                continue
            existing = mapping.get(key)
            if existing is not None:
                collisions.add(key)
                continue
            mapping[key] = row

    for key in collisions:
        mapping.pop(key, None)

    if collisions:
        LOGGER.warning(
            "LigaInsider Name-Kollisionen im Snapshot erkannt: %s Namen werden fuer Birthdate-Mapping ignoriert.",
            len(collisions),
        )
    return mapping


def resolve_player_identity(
    *,
    player: PlayerMaster,
    existing_identity: Any,
    ligainsider_name_map: dict[str, dict[str, Any]],
    ligainsider_profile_cache: dict[str, dict[str, Any]],
    ligainsider_session: requests.Session,
    ligainsider_timeout_seconds: float,
) -> dict[str, Any]:
    birth_date = player.birth_date
    ligainsider_slug = player.ligainsider_player_slug
    ligainsider_player_id = player.ligainsider_player_id
    player_image_url = player.player_image_url

    if existing_identity is not None:
        if birth_date is None:
            birth_date = existing_identity.birth_date
        if not ligainsider_slug:
            ligainsider_slug = existing_identity.ligainsider_player_slug
        if ligainsider_player_id is None:
            ligainsider_player_id = existing_identity.ligainsider_player_id
        if not player_image_url:
            player_image_url = existing_identity.player_image_url

    if not ligainsider_slug:
        li_row = ligainsider_name_map.get(_normalize_name(player.name))
        if li_row is not None:
            ligainsider_slug = _to_text_or_none(li_row.get("ligainsider_player_slug"))
            ligainsider_player_id = _to_int(li_row.get("ligainsider_player_id"))
            if not player_image_url:
                player_image_url = _to_text_or_none(
                    _first_present(
                        li_row,
                        ["player_image_url", "image_url", "photo_url", "player_image"],
                    )
                )

    if ligainsider_slug and (birth_date is None or not player_image_url):
        profile = ligainsider_profile_cache.get(ligainsider_slug)
        if profile is None:
            profile = fetch_ligainsider_profile(
                session=ligainsider_session,
                slug=ligainsider_slug,
                ligainsider_player_id=ligainsider_player_id,
                timeout_seconds=ligainsider_timeout_seconds,
            )
            ligainsider_profile_cache[ligainsider_slug] = profile
        if birth_date is None:
            birth_date = _parse_date(profile.get("birth_date"))
        if not player_image_url:
            player_image_url = _to_text_or_none(profile.get("player_image_url"))

    if existing_identity is not None and birth_date is None:
        player_uid = existing_identity.player_uid
    else:
        player_uid = build_player_uid(player.name, birth_date)

    return {
        "player_uid": player_uid,
        "birth_date": birth_date,
        "ligainsider_player_slug": ligainsider_slug,
        "ligainsider_player_id": ligainsider_player_id,
        "player_image_url": player_image_url,
    }


def fetch_ligainsider_profile(
    *,
    session: requests.Session,
    slug: str,
    ligainsider_player_id: int | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    base_url = os.getenv("LIGAINSIDER_BASE_URL", "https://www.ligainsider.de").rstrip("/")
    url_candidates: list[str] = []
    if ligainsider_player_id is not None:
        url_candidates.append(f"{base_url}/{slug}_{ligainsider_player_id}/")
    url_candidates.append(f"{base_url}/{slug}/")

    headers = {
        "User-Agent": os.getenv("LIGAINSIDER_USER_AGENT", "kickbase-analyzer/1.0"),
        "Accept": "text/html,application/xhtml+xml",
    }
    for url in url_candidates:
        try:
            response = session.get(url, timeout=timeout_seconds, headers=headers)
        except requests.RequestException:
            continue
        if response.status_code >= 400:
            continue
        birth_date = _extract_birth_date_from_ligainsider_html(response.text)
        image_url = _extract_player_image_from_ligainsider_html(response.text, page_url=url)
        if birth_date is not None or image_url:
            return {"birth_date": birth_date, "player_image_url": image_url}
    return {"birth_date": None, "player_image_url": None}


def _extract_birth_date_from_ligainsider_html(html_text: str) -> date | None:
    patterns = [
        r'"birthDate"\s*:\s*"(?P<iso>\d{4}-\d{2}-\d{2})"',
        r"Geburtstag[^0-9]{0,40}(?P<de>\d{2}\.\d{2}\.\d{4})",
        r"geboren[^0-9]{0,40}(?P<de>\d{2}\.\d{2}\.\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match is None:
            continue
        if "iso" in match.groupdict():
            parsed = _parse_date(match.group("iso"))
            if parsed is not None:
                return parsed
        if "de" in match.groupdict():
            parsed = _parse_date(match.group("de"))
            if parsed is not None:
                return parsed
    return None


def _extract_player_image_from_ligainsider_html(html_text: str, *, page_url: str) -> str | None:
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\'](?P<url>[^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\'](?P<url>[^"\']+)["\']',
        r'"image"\s*:\s*"(?P<url>https?://[^"]+)"',
        r'"image"\s*:\s*"(?P<url>/[^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE)
        if match is None:
            continue
        candidate = _to_text_or_none(match.group("url"))
        if not candidate:
            continue
        return urljoin(page_url, candidate)
    return None


def build_player_uid(name: str, birth_date: date | None) -> str:
    normalized_name = _normalize_name(name)
    birth_token = birth_date.strftime("%Y%m%d") if birth_date is not None else "00000000"
    return f"{normalized_name}_{birth_token}"


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = text.strip("_").lower()
    return text or "unknown_player"


def build_match_lookup(payload: dict[str, Any] | list[Any]) -> dict[tuple[int, int, int], dict[str, Any]]:
    lookup: dict[tuple[int, int, int], dict[str, Any]] = {}
    for row in extract_rows(payload):
        day = _to_int(_first_present(row, ["day", "dayNumber", "matchday"]))
        matches = row.get("it") if isinstance(row, dict) else None
        if day is None or not isinstance(matches, list):
            continue
        for match in matches:
            if not isinstance(match, dict):
                continue
            t1 = _to_int(match.get("t1"))
            t2 = _to_int(match.get("t2"))
            if t1 is None or t2 is None:
                continue
            lookup[(day, t1, t2)] = match
    return lookup


def build_team_symbol_lookup(payload: dict[str, Any] | list[Any]) -> dict[int, str]:
    team_code_by_team_id: dict[int, str] = {}
    for row in extract_rows(payload):
        matches = row.get("it") if isinstance(row, dict) else None
        if not isinstance(matches, list):
            continue
        for match in matches:
            if not isinstance(match, dict):
                continue
            t1 = _to_int(match.get("t1"))
            t2 = _to_int(match.get("t2"))
            t1_symbol = _normalize_team_code(_to_text_or_none(match.get("t1sy")))
            t2_symbol = _normalize_team_code(_to_text_or_none(match.get("t2sy")))
            if t1 is not None and t1_symbol is not None:
                team_code_by_team_id[t1] = t1_symbol
            if t2 is not None and t2_symbol is not None:
                team_code_by_team_id[t2] = t2_symbol
    return team_code_by_team_id


def _resolve_team_code(
    *,
    team_id: int | None,
    inline_symbol: str | None,
    team_code_by_team_id: dict[int, str] | None,
) -> str | None:
    if inline_symbol:
        return _normalize_team_code(inline_symbol)
    if team_id is not None and team_code_by_team_id is not None:
        mapped = team_code_by_team_id.get(team_id)
        if mapped:
            return _normalize_team_code(mapped)
    if team_id is not None:
        return f"T{team_id}"
    return None


def _normalize_team_code(value: str | None) -> str | None:
    if not value:
        return None
    normalized = re.sub(r"[^A-Za-z0-9]+", "", value).upper()
    return normalized or None


def _season_token(season_label: str) -> str:
    match = re.match(r"^\s*(\d{4})\s*/\s*(\d{4})\s*$", season_label)
    if match is None:
        return season_label or "unknown"
    left = match.group(1)[2:]
    right = match.group(2)[2:]
    return f"{left}/{right}"


def format_team_name(team_code: str | None, team_name: str | None) -> str | None:
    if not team_name and team_code:
        team_name = DEFAULT_TEAM_NAME_BY_CODE.get(team_code)
    if team_code and team_name:
        if re.match(r"^[A-Z0-9]{2,5}\s+\(.+\)$", team_name):
            return team_name
        return f"{team_code} ({team_name})"
    if team_code:
        return team_code
    if team_name:
        return team_name
    return None


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
    player_uid: str,
    kb_player_id: int,
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
                "player_uid": player_uid,
                "kb_player_id": kb_player_id,
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
    player_uid: str,
    kb_player_id: int,
    competition_id: int,
    active_season_label: str,
    match_lookup: dict[tuple[int, int, int], dict[str, Any]] | None = None,
    team_code_by_team_id: dict[int, str] | None = None,
) -> list[dict[str, Any]]:
    rows = _extract_performance_entries(payload)
    out: list[dict[str, Any]] = []

    for row, season_label in rows:
        season_label_safe = season_label or "unknown"
        matchday = _to_int(_first_present(row, ["matchday", "dayNumber", "spieltag", "day"]))
        points_total = _to_int(_first_present(row, ["p", "points", "pts", "total_points"]))
        if matchday is None or points_total is None:
            continue

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

        if (
            match_lookup is not None
            and season_label_safe == active_season_label
            and matchday is not None
            and t1 is not None
            and t2 is not None
        ):
            lookup_match = match_lookup.get((matchday, t1, t2))
            if lookup_match is None:
                lookup_match = match_lookup.get((matchday, t2, t1))
            if isinstance(lookup_match, dict):
                if row.get("match_id") is None:
                    row["match_id"] = _to_int(lookup_match.get("mi"))

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

        t1_code = _resolve_team_code(
            team_id=t1,
            inline_symbol=_to_text_or_none(_first_present(row, ["t1sy", "team_home_symbol"])),
            team_code_by_team_id=team_code_by_team_id,
        )
        t2_code = _resolve_team_code(
            team_id=t2,
            inline_symbol=_to_text_or_none(_first_present(row, ["t2sy", "team_away_symbol"])),
            team_code_by_team_id=team_code_by_team_id,
        )

        match_uid = _build_match_uid(
            season_label=season_label_safe,
            matchday=matchday,
            t1_code=t1_code,
            t2_code=t2_code,
        )

        out.append(
            {
                "player_uid": player_uid,
                "kb_player_id": kb_player_id,
                "competition_id": competition_id,
                "season_label": season_label_safe,
                "matchday": matchday,
                "match_uid": match_uid,
                "match_id": _to_int(_first_present(row, ["match_id", "matchId", "mid", "m" ])),
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
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
    player_uid: str,
    kb_player_id: int,
    competition_id: int,
    season_label: str,
    matchday: int,
    match_uid: str | None,
    event_type_name_map: dict[int, str],
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
            kb_player_id=kb_player_id,
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
                "player_uid": player_uid,
                "kb_player_id": kb_player_id,
                "competition_id": competition_id,
                "season_label": season_label,
                "matchday": matchday,
                "match_uid": match_uid,
                "event_type_id": event_type_id,
                "event_name": event_type_name_map.get(event_type_id, f"event_{event_type_id}"),
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
    kb_player_id: int,
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
            str(kb_player_id),
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


def _build_match_uid(
    *,
    season_label: str,
    matchday: int,
    t1_code: str | None,
    t2_code: str | None,
) -> str:
    season = _season_token(season_label or "unknown")
    left = _normalize_team_code(t1_code) or "TNA"
    right = _normalize_team_code(t2_code) or "TNA"
    matchup = "".join(sorted([left, right]))
    return f"{season}-MD{int(matchday):02d}-{matchup}"


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


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


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
            get_existing_player_identity_by_kb_player_id,
            get_max_market_value_date,
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
    ligainsider_name_map = load_latest_ligainsider_name_map(Path("data/bronze"))
    ligainsider_profile_cache: dict[str, dict[str, Any]] = {}
    ligainsider_session = requests.Session()
    ligainsider_timeout_seconds = float(os.getenv("LIGAINSIDER_TIMEOUT_SECONDS", "15"))

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
    match_lookup = build_match_lookup(matchdays_payload)
    team_code_by_team_id = build_team_symbol_lookup(matchdays_payload)

    event_types_payload = client.get_event_types(token)
    event_types = parse_event_types(event_types_payload)
    event_type_name_map = {
        int(row["event_type_id"]): str(row["name"])
        for row in event_types
        if row.get("event_type_id") is not None
    }

    with get_connection(db_config) as conn:
        inserted, updated = upsert_event_types(conn, event_types)
        summary["event_types_inserted"] += inserted
        summary["event_types_updated"] += updated
        set_state(conn, "last_eventtypes_sync_ts", datetime.now(UTC).isoformat().replace("+00:00", "Z"))
        conn.commit()

        for idx, player in enumerate(players, start=1):
            player_competition_id = player.competition_id or competition_id
            if player_competition_id is None:
                LOGGER.warning("Skip player %s: no competition id", player.kb_player_id)
                continue

            LOGGER.info(
                "[%s/%s] player=%s (%s) competition=%s",
                idx,
                len(players),
                player.name,
                player.kb_player_id,
                player_competition_id,
            )
            existing_identity = get_existing_player_identity_by_kb_player_id(conn, player.kb_player_id)
            identity = resolve_player_identity(
                player=player,
                existing_identity=existing_identity,
                ligainsider_name_map=ligainsider_name_map,
                ligainsider_profile_cache=ligainsider_profile_cache,
                ligainsider_session=ligainsider_session,
                ligainsider_timeout_seconds=ligainsider_timeout_seconds,
            )
            player_uid = str(identity["player_uid"])
            team_code = _resolve_team_code(
                team_id=player.team_id,
                inline_symbol=None,
                team_code_by_team_id=team_code_by_team_id,
            )
            team_name = format_team_name(team_code, player.team_name)

            upsert_players(
                conn,
                [
                    {
                        "player_uid": player_uid,
                        "kb_player_id": player.kb_player_id,
                        "name": player.name,
                        "birth_date": identity["birth_date"],
                        "ligainsider_player_slug": identity["ligainsider_player_slug"],
                        "ligainsider_player_id": identity["ligainsider_player_id"],
                        "player_image_url": identity["player_image_url"],
                        "team_id": player.team_id,
                        "team_code": team_code,
                        "team_name": team_name,
                        "position": player.position,
                        "competition_id": player_competition_id,
                    }
                ],
            )

            market_payload = client.get_market_value_history(
                token,
                player_competition_id,
                player.kb_player_id,
                args.timeframe_days,
            )
            if args.save_raw:
                _write_json(
                    raw_root
                    / str(player.kb_player_id)
                    / f"marketvalue_{player_competition_id}_{args.timeframe_days}.json",
                    market_payload,
                )

            market_rows = parse_market_value_history(
                market_payload,
                player_uid=player_uid,
                kb_player_id=player.kb_player_id,
            )
            max_existing_date = get_max_market_value_date(conn, player_uid)
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
                player.kb_player_id,
            )
            if args.save_raw:
                _write_json(
                    raw_root / str(player.kb_player_id) / f"performance_{player_competition_id}.json",
                    performance_payload,
                )

            perf_rows = parse_performance_rows(
                performance_payload,
                player_uid=player_uid,
                kb_player_id=player.kb_player_id,
                competition_id=player_competition_id,
                active_season_label=season_label,
                match_lookup=match_lookup,
                team_code_by_team_id=team_code_by_team_id,
            )
            inserted_perf, updated_perf = upsert_match_performance(conn, perf_rows)
            summary["performance_inserted"] += inserted_perf
            summary["performance_updated"] += updated_perf

            match_uid_by_day = {
                int(row["matchday"]): str(row["match_uid"])
                for row in perf_rows
                if str(row.get("season_label")) == season_label
            }
            known_event_type_ids = {int(row["event_type_id"]) for row in event_types}

            for day_number in range(args.days_from, days_to + 1):
                playercenter_payload = client.get_playercenter(
                    token,
                    player_competition_id,
                    player.kb_player_id,
                    day_number,
                )
                if args.save_raw:
                    _write_json(
                        raw_root
                        / str(player.kb_player_id)
                        / f"playercenter_{player_competition_id}_day_{day_number}.json",
                        playercenter_payload,
                    )

                event_rows = parse_playercenter_events(
                    playercenter_payload,
                    player_uid=player_uid,
                    kb_player_id=player.kb_player_id,
                    competition_id=player_competition_id,
                    season_label=season_label,
                    matchday=day_number,
                    match_uid=match_uid_by_day.get(day_number),
                    event_type_name_map=event_type_name_map,
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
                        for row in dynamic_event_types:
                            event_type_name_map[int(row["event_type_id"])] = str(row["name"])

                inserted_events = insert_match_events(conn, event_rows)
                summary["match_events_inserted"] += inserted_events
            deleted_events = cleanup_unknown_event_rows_for_player(
                conn,
                player_uid=player_uid,
                competition_id=player_competition_id,
                season_label=season_label,
            )
            summary["match_events_unknown_cleanup_deleted"] += deleted_events

            set_state(
                conn,
                f"last_marketvalue_date_{player_uid}",
                str(summary["latest_marketvalue_date"] or ""),
            )
            set_state(
                conn,
                f"last_matchday_processed_{player_competition_id}_{season_label}_{player_uid}",
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
