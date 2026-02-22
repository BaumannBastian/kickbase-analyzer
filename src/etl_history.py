# ------------------------------------
# etl_history.py
#
# ETL-Pipeline fuer Kickbase-History in PostgreSQL (RAW-Star-Schema).
# Laedt Spielertreiber aus Databricks oder CSV und schreibt:
# - Marktwert-Historie
# - Match-Performance
# - Event-Breakdowns
# - Spielerbilder als BYTEA (JPG/PNG)
#
# Usage
# ------------------------------------
# - python -m src.etl_history --players-csv ./in/players.csv --max-players 1
# - python -m src.etl_history --max-players 25 --days-from 1
# - python -m src.etl_history --players-csv ./in/players.csv --export-dir ./out/postgres_export
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
import time
from typing import Any, Iterable
from urllib.parse import urljoin
import unicodedata

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


LOGGER = logging.getLogger("kickbase_history_etl")

RETRYABLE_IMAGE_STATUS_CODES = {429, 500, 502, 503, 504}
ALLOWED_IMAGE_MIME_TYPES = {"image/jpeg", "image/png"}

DEFAULT_TEAM_FULL_NAME_BY_CODE = {
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
    player_uid: int
    kb_player_id: int | None
    player_name: str
    kickbase_team_id: int | None
    team_code: str | None
    team_name: str | None
    position: str | None
    league_key: str | None
    competition_id: int | None
    ligainsider_player_slug: str | None
    ligainsider_player_id: int | None
    birthdate: date | None
    image_url: str | None


@dataclass(frozen=True)
class ImageLoadResult:
    image_blob: bytes | None
    image_mime: str | None
    image_sha256: str | None
    status: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kickbase history ETL -> PostgreSQL RAW schema")
    parser.add_argument("--env-file", default=".env", help="Pfad zur .env Datei")
    parser.add_argument("--players-csv", default=None, help="Fallback CSV mit player_uid/kb_player_id")
    parser.add_argument("--competition-id", type=int, default=None)
    parser.add_argument("--competition-name", default="Bundesliga")
    parser.add_argument("--league-id", default=os.getenv("KICKBASE_LEAGUE_ID", ""))
    parser.add_argument("--league-key", default=os.getenv("KICKBASE_LEAGUE_KEY", "bundesliga_1"))
    parser.add_argument("--max-players", type=int, default=None)
    parser.add_argument(
        "--player-id",
        type=int,
        action="append",
        default=[],
        help="Filter auf kb_player_id (mehrfach nutzbar)",
    )
    parser.add_argument(
        "--player-uid",
        type=int,
        action="append",
        default=[],
        help="Filter auf internen player_uid (mehrfach nutzbar)",
    )
    parser.add_argument("--player-name-like", default=None)
    parser.add_argument("--rps", type=float, default=3.0)
    parser.add_argument("--timeframe-days", type=int, default=3650)
    parser.add_argument("--days-from", type=int, default=1)
    parser.add_argument("--days-to", type=int, default=None)
    parser.add_argument("--season-label", default=None, help="Override fuer Saisonlabel, z.B. 2025/2026")
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--raw-dir", default="out/raw")
    parser.add_argument("--databricks-query", default=None)

    parser.add_argument("--skip-image-download", action="store_true")
    parser.add_argument("--image-timeout-seconds", type=float, default=float(os.getenv("IMAGE_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--image-max-bytes", type=int, default=int(os.getenv("IMAGE_MAX_BYTES", str(5 * 1024 * 1024))))
    parser.add_argument("--image-retries", type=int, default=int(os.getenv("IMAGE_RETRIES", "2")))
    parser.add_argument("--image-backoff-seconds", type=float, default=float(os.getenv("IMAGE_BACKOFF_SECONDS", "1")))
    parser.add_argument("--image-cache-dir", default=os.getenv("IMAGE_CACHE_DIR", ".cache/player_images"))

    parser.add_argument("--export-dir", default=None, help="Optionales Exportverzeichnis fuer CSV/Bilder")
    parser.add_argument("--export-tables", action="store_true", help="Exportiert Tabellen als CSV (ohne image_blob)")
    parser.add_argument("--export-images", action="store_true", help="Exportiert image_blob nach Dateien + Mapping CSV")

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
        CAST(kickbase_player_id AS BIGINT) AS player_uid,
        CAST(kickbase_player_id AS BIGINT) AS kb_player_id,
        player_name,
        CAST(team_id AS BIGINT) AS kickbase_team_id,
        CAST(NULL AS STRING) AS team_code,
        CAST(NULL AS STRING) AS team_name,
        position,
        CAST(NULL AS STRING) AS league_key,
        CAST(NULL AS BIGINT) AS competition_id,
        CAST(NULL AS STRING) AS ligainsider_player_slug,
        CAST(NULL AS BIGINT) AS ligainsider_player_id,
        CAST(NULL AS DATE) AS birthdate,
        CAST(NULL AS STRING) AS image_url
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
            columns = [str(desc[0]).lower() for desc in cur.description]
            for raw in cur.fetchall():
                row = dict(zip(columns, raw, strict=False))
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
    kb_player_id = _to_int(
        _first_present(row, ["kb_player_id", "kickbase_player_id", "player_id", "id", "pi"])
    )
    player_uid = _to_int(_first_present(row, ["player_uid", "uid", "internal_player_uid"]))
    if player_uid is None and kb_player_id is not None:
        player_uid = kb_player_id
    if player_uid is None:
        return None

    player_name = _to_text(_first_present(row, ["player_name", "name", "n"])) or f"player_{player_uid}"

    return PlayerMaster(
        player_uid=player_uid,
        kb_player_id=kb_player_id,
        player_name=player_name,
        kickbase_team_id=_to_int(_first_present(row, ["kickbase_team_id", "team_id", "tid", "t"])),
        team_code=_normalize_team_code(_to_text_or_none(_first_present(row, ["team_code", "team_symbol", "team_short"]))),
        team_name=_to_text_or_none(_first_present(row, ["team_name", "team"])),
        position=_to_text_or_none(_first_present(row, ["position", "pos"])),
        league_key=_to_text_or_none(_first_present(row, ["league_key", "league", "competition_key"])),
        competition_id=_to_int(_first_present(row, ["competition_id", "competitionid", "cpi"])),
        ligainsider_player_slug=_to_text_or_none(
            _first_present(row, ["ligainsider_player_slug", "li_player_slug", "ligainsider_slug"])
        ),
        ligainsider_player_id=_to_int(
            _first_present(row, ["ligainsider_player_id", "li_player_id", "ligainsider_id"])
        ),
        birthdate=_parse_date(_first_present(row, ["birthdate", "birth_date", "birthday", "dob"])),
        image_url=_normalize_image_url(
            _to_text_or_none(
                _first_present(
                    row,
                    [
                        "image_url",
                        "player_image_url",
                        "image",
                        "player_image",
                        "pim",
                    ],
                )
            )
        ),
    )


def select_players(players: list[PlayerMaster], args: argparse.Namespace) -> list[PlayerMaster]:
    out = players

    if args.player_uid:
        wanted_uid = set(args.player_uid)
        out = [player for player in out if player.player_uid in wanted_uid]

    if args.player_id:
        wanted_kb = set(args.player_id)
        out = [
            player for player in out if player.kb_player_id is not None and player.kb_player_id in wanted_kb
        ]

    if args.player_name_like:
        needle = args.player_name_like.strip().lower()
        out = [player for player in out if needle in player.player_name.lower()]

    out = sorted(out, key=lambda player: (player.player_name.lower(), player.player_uid))

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

            if key in mapping:
                collisions.add(key)
                continue
            mapping[key] = row

    for key in collisions:
        mapping.pop(key, None)

    if collisions:
        LOGGER.warning(
            "LigaInsider Name-Kollisionen im Snapshot erkannt: %s Namen werden ignoriert.",
            len(collisions),
        )

    return mapping


def resolve_player_enrichment(
    *,
    player: PlayerMaster,
    existing_identity: Any,
    ligainsider_name_map: dict[str, dict[str, Any]],
    ligainsider_profile_cache: dict[str, dict[str, Any]],
    ligainsider_session: requests.Session,
    ligainsider_timeout_seconds: float,
) -> dict[str, Any]:
    birthdate = player.birthdate
    image_url = player.image_url
    ligainsider_slug = player.ligainsider_player_slug
    ligainsider_player_id = player.ligainsider_player_id

    if existing_identity is not None and birthdate is None:
        birthdate = existing_identity.birthdate

    if not ligainsider_slug:
        li_row = ligainsider_name_map.get(_normalize_name(player.player_name))
        if li_row is not None:
            ligainsider_slug = _to_text_or_none(li_row.get("ligainsider_player_slug"))
            ligainsider_player_id = _to_int(li_row.get("ligainsider_player_id"))
            if image_url is None:
                image_url = _normalize_image_url(
                    _to_text_or_none(
                        _first_present(
                            li_row,
                            [
                                "player_image_url",
                                "image_url",
                                "player_image",
                                "photo_url",
                            ],
                        )
                    )
                )

    if ligainsider_slug and (birthdate is None or image_url is None):
        profile = ligainsider_profile_cache.get(ligainsider_slug)
        if profile is None:
            profile = fetch_ligainsider_profile(
                session=ligainsider_session,
                slug=ligainsider_slug,
                ligainsider_player_id=ligainsider_player_id,
                timeout_seconds=ligainsider_timeout_seconds,
            )
            ligainsider_profile_cache[ligainsider_slug] = profile

        if birthdate is None:
            birthdate = _parse_date(profile.get("birthdate"))
        if image_url is None:
            image_url = _normalize_image_url(_to_text_or_none(profile.get("image_url")))

    return {
        "birthdate": birthdate,
        "image_url": image_url,
        "ligainsider_player_slug": ligainsider_slug,
        "ligainsider_player_id": ligainsider_player_id,
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

        birthdate = _extract_birth_date_from_ligainsider_html(response.text)
        image_url = _extract_player_image_from_ligainsider_html(response.text, page_url=url)
        if birthdate is not None or image_url is not None:
            return {
                "birthdate": birthdate,
                "image_url": image_url,
            }

    return {
        "birthdate": None,
        "image_url": None,
    }


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
        return _normalize_image_url(urljoin(page_url, candidate))

    return None


def load_player_image_blob(
    *,
    session: requests.Session,
    player_uid: int,
    image_url: str | None,
    existing_sha256: str | None,
    cache_dir: Path,
    timeout_seconds: float,
    max_bytes: int,
    retries: int,
    backoff_seconds: float,
) -> ImageLoadResult:
    if not image_url:
        return ImageLoadResult(
            image_blob=None,
            image_mime=None,
            image_sha256=None,
            status="missing_source",
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_meta_path = cache_dir / f"{player_uid}.json"
    cache_blob_path = cache_dir / f"{player_uid}.bin"

    cached = _load_image_from_cache(
        cache_meta_path=cache_meta_path,
        cache_blob_path=cache_blob_path,
        expected_url=image_url,
    )
    if cached is not None:
        image_bytes, image_mime = cached
        image_sha256 = hashlib.sha256(image_bytes).hexdigest()
        if existing_sha256 and existing_sha256 == image_sha256:
            return ImageLoadResult(None, image_mime, image_sha256, "unchanged")
        return ImageLoadResult(image_bytes, image_mime, image_sha256, "updated_from_cache")

    image_bytes, image_mime, status = _download_image_with_retries(
        session=session,
        url=image_url,
        timeout_seconds=timeout_seconds,
        max_bytes=max_bytes,
        retries=retries,
        backoff_seconds=backoff_seconds,
    )
    if image_bytes is None or image_mime is None:
        return ImageLoadResult(None, None, None, status)

    image_sha256 = hashlib.sha256(image_bytes).hexdigest()
    _write_image_cache(
        cache_meta_path=cache_meta_path,
        cache_blob_path=cache_blob_path,
        image_url=image_url,
        image_mime=image_mime,
        image_sha256=image_sha256,
        image_bytes=image_bytes,
    )

    if existing_sha256 and existing_sha256 == image_sha256:
        return ImageLoadResult(None, image_mime, image_sha256, "unchanged")

    return ImageLoadResult(image_bytes, image_mime, image_sha256, "updated")


def _load_image_from_cache(
    *,
    cache_meta_path: Path,
    cache_blob_path: Path,
    expected_url: str,
) -> tuple[bytes, str] | None:
    if not cache_meta_path.exists() or not cache_blob_path.exists():
        return None

    try:
        meta = json.loads(cache_meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    if not isinstance(meta, dict):
        return None

    cached_url = _to_text_or_none(meta.get("image_url"))
    cached_mime = _to_text_or_none(meta.get("image_mime"))
    if cached_url != expected_url or cached_mime not in ALLOWED_IMAGE_MIME_TYPES:
        return None

    try:
        image_bytes = cache_blob_path.read_bytes()
    except OSError:
        return None

    if not image_bytes:
        return None

    return image_bytes, str(cached_mime)


def _write_image_cache(
    *,
    cache_meta_path: Path,
    cache_blob_path: Path,
    image_url: str,
    image_mime: str,
    image_sha256: str,
    image_bytes: bytes,
) -> None:
    cache_blob_path.write_bytes(image_bytes)
    cache_meta_path.write_text(
        json.dumps(
            {
                "image_url": image_url,
                "image_mime": image_mime,
                "image_sha256": image_sha256,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _download_image_with_retries(
    *,
    session: requests.Session,
    url: str,
    timeout_seconds: float,
    max_bytes: int,
    retries: int,
    backoff_seconds: float,
) -> tuple[bytes | None, str | None, str]:
    headers = {
        "User-Agent": os.getenv("KICKBASE_IMAGE_USER_AGENT", "kickbase-analyzer/1.0"),
        "Accept": "image/png,image/jpeg,image/*;q=0.9,*/*;q=0.2",
    }

    for attempt in range(retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=timeout_seconds, stream=True)
        except requests.RequestException:
            if attempt < retries:
                time.sleep(backoff_seconds * (2**attempt))
                continue
            return None, None, "failed_request"

        if response.status_code in RETRYABLE_IMAGE_STATUS_CODES and attempt < retries:
            time.sleep(backoff_seconds * (2**attempt))
            continue

        if response.status_code >= 400:
            return None, None, f"failed_status_{response.status_code}"

        image_mime = _normalize_image_mime(
            _to_text_or_none(response.headers.get("Content-Type")),
            url=url,
        )
        if image_mime not in ALLOWED_IMAGE_MIME_TYPES:
            return None, None, "failed_mime"

        content_length = _to_int(response.headers.get("Content-Length"))
        if content_length is not None and content_length > max_bytes:
            return None, None, "failed_too_large"

        payload = bytearray()
        try:
            for chunk in response.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                payload.extend(chunk)
                if len(payload) > max_bytes:
                    return None, None, "failed_too_large"
        finally:
            response.close()

        if not payload:
            return None, None, "failed_empty"

        return bytes(payload), image_mime, "downloaded"

    return None, None, "failed_unknown"


def _normalize_image_mime(content_type: str | None, *, url: str) -> str | None:
    if content_type:
        normalized = content_type.split(";", 1)[0].strip().lower()
        if normalized in ALLOWED_IMAGE_MIME_TYPES:
            return normalized

    lower_url = url.lower()
    if lower_url.endswith(".jpg") or lower_url.endswith(".jpeg"):
        return "image/jpeg"
    if lower_url.endswith(".png"):
        return "image/png"
    return None


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
        event_type_id = _to_int(_first_present(row, ["eti", "eventTypeId", "event_type_id", "id", "i"]))
        if event_type_id is None:
            continue
        rows.append(
            {
                "event_type_id": event_type_id,
                "event_name": _to_text(_first_present(row, ["name", "n", "title", "ti"]))
                or f"event_{event_type_id}",
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
                rows.append({"event_type_id": event_type_id, "event_name": value, "template": None})
            elif isinstance(value, dict):
                rows.append(
                    {
                        "event_type_id": event_type_id,
                        "event_name": _to_text(_first_present(value, ["name", "n", "title", "ti"]))
                        or f"event_{event_type_id}",
                        "template": _to_text_or_none(_first_present(value, ["template", "tpl", "att", "te"])),
                    }
                )

    return _dedupe_by_key(rows, "event_type_id")


def parse_market_value_history(
    payload: dict[str, Any] | list[Any],
    *,
    player_uid: int,
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
    player_uid: int,
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

        kickoff_ts = _parse_datetime(_first_present(row, ["kickoff", "md", "match_ts", "d", "date", "ts"]))
        score_home = _to_int(_first_present(row, ["scoreHome", "score_home", "sh", "t1g"]))
        score_away = _to_int(_first_present(row, ["scoreAway", "score_away", "sa", "t2g"]))

        if score_home is None or score_away is None:
            score_text = _to_text(_first_present(row, ["score", "result", "res"]))
            parsed_score = _parse_score(score_text)
            if parsed_score is not None:
                score_home, score_away = parsed_score

        team_id = _to_int(_first_present(row, ["team_id", "kickbase_team_id", "tid", "t", "pt"]))
        t1 = _to_int(_first_present(row, ["t1", "team_home_id"]))
        t2 = _to_int(_first_present(row, ["t2", "team_away_id"]))

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
                if kickoff_ts is None:
                    kickoff_ts = _parse_datetime(lookup_match.get("dt"))
                if score_home is None:
                    score_home = _to_int(lookup_match.get("t1g"))
                if score_away is None:
                    score_away = _to_int(lookup_match.get("t2g"))

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

        opponent_team_id = _to_int(
            _first_present(row, ["opponent_team_id", "opp_tid", "otid", "opponentTeamId"])
        )
        if opponent_team_id is None and team_id is not None and t1 is not None and t2 is not None:
            if team_id == t1:
                opponent_team_id = t2
            elif team_id == t2:
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
                "player_uid": player_uid,
                "season_label": season_label_safe,
                "matchday": matchday,
                "match_uid": match_uid,
                "kickbase_match_id": _to_int(_first_present(row, ["match_id", "matchId", "mid", "m"])),
                "kickoff_ts": kickoff_ts,
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "t1_id": t1,
                "t2_id": t2,
                "t1_code": t1_code,
                "t2_code": t2_code,
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
    player_uid: int,
    match_uid: str | None,
    event_type_name_map: dict[int, str],
) -> list[dict[str, Any]]:
    if not match_uid:
        return []

    event_dicts = _find_event_dicts(payload)
    rows: list[dict[str, Any]] = []

    for index, event in enumerate(event_dicts):
        event_type_id = _to_int(_first_present(event, ["eti", "eventTypeId", "event_type_id", "id"]))
        points = _to_int(_first_present(event, ["p", "points", "pts", "value"]))
        if event_type_id is None or points is None:
            continue

        mt = _to_int(_first_present(event, ["mt", "minute", "min"]))
        att = _to_text_or_none(_first_present(event, ["att", "text", "label", "template"]))
        source_event_id = _to_text_or_none(_first_present(event, ["ei", "event_id", "id"]))

        event_hash = _build_event_hash(
            player_uid=player_uid,
            match_uid=match_uid,
            event_type_id=event_type_id,
            points=points,
            mt=mt,
            att=att,
            source_event_id=source_event_id,
            event_index=index,
        )

        rows.append(
            {
                "event_hash": event_hash,
                "player_uid": player_uid,
                "match_uid": match_uid,
                "event_type_id": event_type_id,
                "event_name": event_type_name_map.get(event_type_id, f"event_{event_type_id}"),
                "points": points,
                "mt": mt,
                "att": att,
                "raw_event": event,
            }
        )

    unique: dict[str, dict[str, Any]] = {}
    for row in rows:
        unique[row["event_hash"]] = row
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


def _build_event_hash(
    *,
    player_uid: int,
    match_uid: str,
    event_type_id: int,
    points: int,
    mt: int | None,
    att: str | None,
    source_event_id: str | None,
    event_index: int,
) -> str:
    raw = "|".join(
        [
            str(player_uid),
            match_uid,
            str(event_type_id),
            str(points),
            "" if mt is None else str(mt),
            "" if att is None else att,
            "" if source_event_id is None else source_event_id,
            str(event_index),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


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
    matchup = f"{left}{right}"
    return f"{season}-MD{int(matchday):02d}-{matchup}"


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^A-Za-z0-9]+", "_", text)
    text = text.strip("_").lower()
    return text or "unknown_player"


def _normalize_image_url(value: str | None) -> str | None:
    if value is None:
        return None

    text = value.strip()
    if not text:
        return None

    if text.startswith("http://") or text.startswith("https://"):
        return text

    if text.startswith("//"):
        return f"https:{text}"

    base_url = os.getenv("KICKBASE_CONTENT_BASE_URL", "https://api.kickbase.com/").rstrip("/") + "/"
    return urljoin(base_url, text.lstrip("/"))


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


def _collect_team_rows(
    *,
    player: PlayerMaster,
    perf_rows: list[dict[str, Any]],
    team_code_by_team_id: dict[int, str],
) -> list[dict[str, Any]]:
    rows_by_key: dict[tuple[int | None, str | None], dict[str, Any]] = {}

    def _add(kickbase_team_id: int | None, team_code: str | None, team_name: str | None) -> None:
        normalized_code = _normalize_team_code(team_code)
        if kickbase_team_id is None and normalized_code is None:
            return
        display_name = _format_team_display_name(
            team_code=normalized_code,
            team_name=team_name,
            fallback_name=DEFAULT_TEAM_FULL_NAME_BY_CODE.get(normalized_code) if normalized_code else None,
        )
        rows_by_key[(kickbase_team_id, normalized_code)] = {
            "kickbase_team_id": kickbase_team_id,
            "team_code": normalized_code,
            "team_name": display_name,
        }

    _add(player.kickbase_team_id, player.team_code, player.team_name)

    for team_id, code in team_code_by_team_id.items():
        _add(team_id, code, DEFAULT_TEAM_FULL_NAME_BY_CODE.get(code))

    for perf in perf_rows:
        is_home = perf.get("is_home")
        team_id = _to_int(perf.get("team_id"))
        opponent_team_id = _to_int(perf.get("opponent_team_id"))
        t1_code = _to_text_or_none(perf.get("t1_code"))
        t2_code = _to_text_or_none(perf.get("t2_code"))

        if is_home is True:
            _add(team_id, t1_code, None)
            _add(opponent_team_id, t2_code, None)
        elif is_home is False:
            _add(team_id, t2_code, None)
            _add(opponent_team_id, t1_code, None)

        _add(_to_int(perf.get("t1_id")), _to_text_or_none(perf.get("t1_code")), None)
        _add(_to_int(perf.get("t2_id")), _to_text_or_none(perf.get("t2_code")), None)

    return list(rows_by_key.values())


def _resolve_team_uid(
    *,
    kickbase_team_id: int | None,
    team_code: str | None,
    team_lookup: Any,
) -> int | None:
    if kickbase_team_id is not None:
        mapped = team_lookup.by_kickbase_team_id.get(kickbase_team_id)
        if mapped is not None:
            return mapped

    normalized = _normalize_team_code(team_code)
    if normalized is not None:
        mapped = team_lookup.by_team_code.get(normalized)
        if mapped is not None:
            return mapped

    return None


def _format_team_display_name(
    *,
    team_code: str | None,
    team_name: str | None,
    fallback_name: str | None,
) -> str | None:
    code = _normalize_team_code(team_code)
    name = _to_text_or_none(team_name) or _to_text_or_none(fallback_name)

    if code and name:
        if re.match(r"^[A-Z0-9]{2,5}\s+\(.+\)$", name):
            return name
        return f"{code} ({name})"
    if code:
        return code
    return name


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging(args.log_level)

    try:
        from src.db import (
            DbConfig,
            ensure_season,
            export_player_images,
            export_raw_tables_to_csv,
            get_connection,
            get_existing_player_identity,
            get_max_market_value_date,
            insert_fact_player_events,
            set_state,
            upsert_bridge_player_team,
            upsert_dim_event_types,
            upsert_dim_matches,
            upsert_dim_players,
            upsert_dim_teams,
            upsert_fact_player_match,
            upsert_market_values,
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

    image_session = requests.Session()
    image_cache_dir = Path(args.image_cache_dir)

    kb_config = KickbaseApiConfig.from_env(rps=args.rps)
    if not kb_config.email or not kb_config.password:
        raise RuntimeError("KICKBASE_EMAIL/KICKBASE_PASSWORD sind nicht gesetzt")

    db_config = DbConfig.from_env()
    raw_root = Path(args.raw_dir)

    summary: dict[str, Any] = {
        "players_processed": 0,
        "event_types_inserted": 0,
        "event_types_updated": 0,
        "teams_inserted": 0,
        "teams_updated": 0,
        "matches_inserted": 0,
        "matches_updated": 0,
        "bridge_inserted": 0,
        "bridge_updated": 0,
        "marketvalue_inserted": 0,
        "marketvalue_updated": 0,
        "player_match_inserted": 0,
        "player_match_updated": 0,
        "player_event_inserted": 0,
        "images_updated": 0,
        "images_unchanged": 0,
        "images_missing": 0,
        "images_failed": 0,
        "earliest_marketvalue_date": None,
        "latest_marketvalue_date": None,
        "csv_exports": [],
        "image_exports": [],
        "image_mapping_csv": None,
    }

    client = KickbaseClient(kb_config)
    token = client.authenticate()

    competition_id = args.competition_id
    if competition_id is None:
        explicit_competitions = sorted(
            {player.competition_id for player in players if player.competition_id is not None}
        )
        if len(explicit_competitions) == 1:
            competition_id = explicit_competitions[0]

    if competition_id is None:
        competition_id = client.discover_competition_id(
            token=token,
            league_id=args.league_id,
            competition_name=args.competition_name,
        )

    if competition_id is None:
        raise RuntimeError("Competition ID konnte nicht ermittelt werden. Bitte --competition-id setzen.")

    matchdays_payload = client.get_matchdays(token, competition_id)
    current_day = detect_current_matchday(matchdays_payload)
    if current_day is None:
        raise RuntimeError("Matchday-Range konnte nicht aus /matchdays gelesen werden")

    days_to = args.days_to if args.days_to is not None else current_day
    if days_to < args.days_from:
        raise RuntimeError("--days-to darf nicht kleiner als --days-from sein")

    active_season_label = args.season_label or detect_season_label(matchdays_payload) or default_season_label()
    if str(active_season_label).strip().lower() in {"", "unknown"}:
        active_season_label = default_season_label()

    league_key = args.league_key.strip() or "bundesliga_1"

    match_lookup = build_match_lookup(matchdays_payload)
    team_code_by_team_id = build_team_symbol_lookup(matchdays_payload)

    event_types_payload = client.get_event_types(token)
    event_types = parse_event_types(event_types_payload)
    event_type_name_map = {
        int(row["event_type_id"]): str(row["event_name"])
        for row in event_types
        if row.get("event_type_id") is not None
    }

    with get_connection(db_config) as conn:
        inserted_types, updated_types = upsert_dim_event_types(conn, event_types)
        summary["event_types_inserted"] += inserted_types
        summary["event_types_updated"] += updated_types

        active_season_uid = ensure_season(conn, league_key=league_key, season_label=active_season_label)
        set_state(conn, "last_eventtypes_sync_ts", datetime.now(UTC).isoformat().replace("+00:00", "Z"))
        conn.commit()

        for index, player in enumerate(players, start=1):
            player_competition_id = player.competition_id or competition_id
            if player_competition_id is None:
                LOGGER.warning("Skip player %s: no competition id", player.player_uid)
                continue

            LOGGER.info(
                "[%s/%s] player=%s (uid=%s, kb=%s) competition=%s",
                index,
                len(players),
                player.player_name,
                player.player_uid,
                player.kb_player_id,
                player_competition_id,
            )

            existing_identity = get_existing_player_identity(
                conn,
                player_uid=player.player_uid,
                kb_player_id=player.kb_player_id,
            )
            enrichment = resolve_player_enrichment(
                player=player,
                existing_identity=existing_identity,
                ligainsider_name_map=ligainsider_name_map,
                ligainsider_profile_cache=ligainsider_profile_cache,
                ligainsider_session=ligainsider_session,
                ligainsider_timeout_seconds=ligainsider_timeout_seconds,
            )

            image_result = ImageLoadResult(None, None, None, "missing_source")
            if args.skip_image_download:
                image_result = ImageLoadResult(None, None, None, "skipped")
            else:
                image_result = load_player_image_blob(
                    session=image_session,
                    player_uid=player.player_uid,
                    image_url=_to_text_or_none(enrichment.get("image_url")),
                    existing_sha256=existing_identity.image_sha256 if existing_identity else None,
                    cache_dir=image_cache_dir,
                    timeout_seconds=args.image_timeout_seconds,
                    max_bytes=args.image_max_bytes,
                    retries=args.image_retries,
                    backoff_seconds=args.image_backoff_seconds,
                )

            if image_result.status in {"updated", "updated_from_cache"}:
                summary["images_updated"] += 1
            elif image_result.status == "unchanged":
                summary["images_unchanged"] += 1
            elif image_result.status in {"missing_source", "skipped"}:
                summary["images_missing"] += 1
            else:
                summary["images_failed"] += 1

            upsert_dim_players(
                conn,
                [
                    {
                        "player_uid": player.player_uid,
                        "kb_player_id": player.kb_player_id,
                        "player_name": player.player_name,
                        "position": player.position,
                        "birthdate": enrichment.get("birthdate"),
                        "image_blob": image_result.image_blob,
                        "image_mime": image_result.image_mime,
                        "image_sha256": image_result.image_sha256,
                    }
                ],
            )

            market_payload = client.get_market_value_history(
                token,
                player_competition_id,
                int(player.kb_player_id or player.player_uid),
                args.timeframe_days,
            )
            if args.save_raw:
                _write_json(
                    raw_root / str(player.player_uid) / f"marketvalue_{player_competition_id}_{args.timeframe_days}.json",
                    market_payload,
                )

            market_rows = parse_market_value_history(market_payload, player_uid=player.player_uid)
            max_existing_date = get_max_market_value_date(conn, player.player_uid)
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
                int(player.kb_player_id or player.player_uid),
            )
            if args.save_raw:
                _write_json(
                    raw_root / str(player.player_uid) / f"performance_{player_competition_id}.json",
                    performance_payload,
                )

            perf_rows = parse_performance_rows(
                performance_payload,
                player_uid=player.player_uid,
                active_season_label=active_season_label,
                match_lookup=match_lookup,
                team_code_by_team_id=team_code_by_team_id,
            )

            team_rows = _collect_team_rows(
                player=player,
                perf_rows=perf_rows,
                team_code_by_team_id=team_code_by_team_id,
            )
            team_lookup, teams_inserted, teams_updated = upsert_dim_teams(
                conn,
                league_key=league_key,
                rows=team_rows,
            )
            summary["teams_inserted"] += teams_inserted
            summary["teams_updated"] += teams_updated

            player_team_uid = _resolve_team_uid(
                kickbase_team_id=player.kickbase_team_id,
                team_code=player.team_code,
                team_lookup=team_lookup,
            )
            if player_team_uid is not None:
                bridge_inserted, bridge_updated = upsert_bridge_player_team(
                    conn,
                    [
                        {
                            "player_uid": player.player_uid,
                            "season_uid": active_season_uid,
                            "team_uid": player_team_uid,
                            "source": "kickbase",
                        }
                    ],
                )
                summary["bridge_inserted"] += bridge_inserted
                summary["bridge_updated"] += bridge_updated

            season_uid_cache: dict[str, int] = {active_season_label: active_season_uid}
            dim_match_rows: list[dict[str, Any]] = []
            fact_match_rows: list[dict[str, Any]] = []
            match_uid_by_day: dict[int, str] = {}

            for perf_row in perf_rows:
                perf_season_label = str(perf_row["season_label"])
                if perf_season_label not in season_uid_cache and perf_season_label.lower() != "unknown":
                    season_uid_cache[perf_season_label] = ensure_season(
                        conn,
                        league_key=league_key,
                        season_label=perf_season_label,
                    )

                t1_team_uid = _resolve_team_uid(
                    kickbase_team_id=_to_int(perf_row.get("t1_id")),
                    team_code=_to_text_or_none(perf_row.get("t1_code")),
                    team_lookup=team_lookup,
                )
                t2_team_uid = _resolve_team_uid(
                    kickbase_team_id=_to_int(perf_row.get("t2_id")),
                    team_code=_to_text_or_none(perf_row.get("t2_code")),
                    team_lookup=team_lookup,
                )

                match_uid = str(perf_row["match_uid"])
                matchday = int(perf_row["matchday"])
                if perf_season_label == active_season_label:
                    match_uid_by_day[matchday] = match_uid

                dim_match_rows.append(
                    {
                        "match_uid": match_uid,
                        "kickbase_match_id": _to_int(perf_row.get("kickbase_match_id")),
                        "league_key": league_key,
                        "season_uid": season_uid_cache.get(perf_season_label),
                        "season_label": perf_season_label,
                        "matchday": matchday,
                        "home_team_uid": t1_team_uid,
                        "away_team_uid": t2_team_uid,
                        "kickoff_ts": perf_row.get("kickoff_ts"),
                        "score_home": _to_int(perf_row.get("score_home")),
                        "score_away": _to_int(perf_row.get("score_away")),
                    }
                )

                fact_match_rows.append(
                    {
                        "player_uid": player.player_uid,
                        "match_uid": match_uid,
                        "points_total": int(perf_row["points_total"]),
                        "is_home": perf_row.get("is_home"),
                        "match_result": _to_text_or_none(perf_row.get("match_result")),
                        "raw_json": perf_row.get("raw_json"),
                    }
                )

            dedup_matches: dict[str, dict[str, Any]] = {}
            for row in dim_match_rows:
                dedup_matches[str(row["match_uid"])] = row

            inserted_matches, updated_matches = upsert_dim_matches(conn, list(dedup_matches.values()))
            summary["matches_inserted"] += inserted_matches
            summary["matches_updated"] += updated_matches

            inserted_fpm, updated_fpm = upsert_fact_player_match(conn, fact_match_rows)
            summary["player_match_inserted"] += inserted_fpm
            summary["player_match_updated"] += updated_fpm

            known_event_type_ids = {int(row["event_type_id"]) for row in event_types}
            for day_number in range(args.days_from, days_to + 1):
                playercenter_payload = client.get_playercenter(
                    token,
                    player_competition_id,
                    int(player.kb_player_id or player.player_uid),
                    day_number,
                )
                if args.save_raw:
                    _write_json(
                        raw_root / str(player.player_uid) / f"playercenter_{player_competition_id}_day_{day_number}.json",
                        playercenter_payload,
                    )

                event_rows = parse_playercenter_events(
                    playercenter_payload,
                    player_uid=player.player_uid,
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
                                "event_type_id": event_type_id,
                                "event_name": f"event_{event_type_id}",
                                "template": None,
                            }
                            for event_type_id in missing_event_type_ids
                        ]
                        inserted_dyn, updated_dyn = upsert_dim_event_types(conn, dynamic_event_types)
                        summary["event_types_inserted"] += inserted_dyn
                        summary["event_types_updated"] += updated_dyn
                        known_event_type_ids.update(missing_event_type_ids)
                        for dynamic in dynamic_event_types:
                            event_type_name_map[int(dynamic["event_type_id"])] = str(dynamic["event_name"])

                inserted_events = insert_fact_player_events(conn, event_rows)
                summary["player_event_inserted"] += inserted_events

            set_state(
                conn,
                f"last_marketvalue_date_{player.player_uid}",
                str(summary["latest_marketvalue_date"] or ""),
            )
            set_state(
                conn,
                f"last_matchday_processed_{league_key}_{active_season_label}_{player.player_uid}",
                str(days_to),
            )

            conn.commit()
            summary["players_processed"] += 1

        should_export_tables = bool(args.export_tables)
        should_export_images = bool(args.export_images)
        if args.export_dir and not should_export_tables and not should_export_images:
            should_export_tables = True
            should_export_images = True

        if args.export_dir:
            export_root = Path(args.export_dir)
            if should_export_tables:
                csv_paths = export_raw_tables_to_csv(conn, export_root / "tables")
                summary["csv_exports"] = [str(path) for path in csv_paths]
            if should_export_images:
                image_paths, mapping_path = export_player_images(conn, export_root / "images")
                summary["image_exports"] = [str(path) for path in image_paths]
                summary["image_mapping_csv"] = str(mapping_path)

    if isinstance(summary["earliest_marketvalue_date"], date):
        summary["earliest_marketvalue_date"] = summary["earliest_marketvalue_date"].isoformat()
    if isinstance(summary["latest_marketvalue_date"], date):
        summary["latest_marketvalue_date"] = summary["latest_marketvalue_date"].isoformat()

    print(json.dumps(summary, ensure_ascii=True, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
