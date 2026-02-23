# ------------------------------------
# run_silver_sync.py
#
# Dieses Skript baut zwei Silver-Snapshots aus Bronze:
# 1) eine gemeinsame Spieler-Tabelle (Kickbase + LigaInsider)
# 2) eine Team-Matchup-Tabelle (Odds + wahrscheinliche Aufstellungen)
#
# Outputs
# ------------------------------------
# 1) data/lakehouse/silver/player_snapshot/snapshot_<ts>.ndjson
# 2) data/lakehouse/silver/team_matchup_snapshot/snapshot_<ts>.ndjson
#
# Usage
# ------------------------------------
# - python -m databricks.jobs.silver_sync.run_silver_sync
# - python -m databricks.jobs.silver_sync.run_silver_sync --timestamp 2026-02-21T131500Z
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
from typing import Any, Sequence
from urllib.parse import urlsplit, urlunsplit
import unicodedata

from databricks.jobs.common_io import (
    find_partitioned_files_for_timestamp,
    latest_timestamp_common_partitioned,
    read_ndjson,
    write_ndjson,
)


REQUIRED_INPUT_DATASETS = [
    "kickbase_player_snapshot",
    "ligainsider_status_snapshot",
]
OPTIONAL_INPUT_DATASET = "odds_match_snapshot"

TEAM_CODE_BY_KICKBASE_TEAM_ID: dict[int, str] = {
    2: "FCB",
    3: "BVB",
    4: "SGE",
    5: "SCF",
    6: "HSV",
    7: "B04",
    8: "S04",
    9: "VFB",
    10: "SVW",
    11: "WOB",
    12: "F95",
    13: "FCA",
    14: "TSG",
    15: "BMG",
    16: "FCN",
    17: "H96",
    18: "M05",
    19: "SGF",
    20: "BSC",
    21: "EBS",
    22: "DSC",
    23: "HRO",
    24: "BOC",
    27: "KSC",
    28: "KOE",
    29: "SCP",
    30: "FCK",
    32: "AUE",
    35: "SVS",
    36: "FCI",
    39: "STP",
    40: "FCU",
    41: "SGD",
    42: "D98",
    43: "RBL",
    47: "REG",
    48: "FCM",
    49: "HEI",
    50: "FCH",
    51: "KIE",
    75: "WIE",
    76: "OSN",
    77: "ELV",
    93: "ULM",
    94: "MUE",
}

TEAM_NAME_BY_CODE: dict[str, str] = {
    "B04": "Bayer Leverkusen",
    "BOC": "VfL Bochum",
    "BMG": "Borussia Moenchengladbach",
    "BVB": "Borussia Dortmund",
    "FCA": "FC Augsburg",
    "FCB": "FC Bayern",
    "FCH": "1. FC Heidenheim",
    "FCK": "1. FC Kaiserslautern",
    "FCN": "1. FC Nuernberg",
    "KOE": "1. FC Koeln",
    "FCU": "Union Berlin",
    "KIE": "Holstein Kiel",
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

TEAM_CODE_BY_LIGAINSIDER_SLUG: dict[str, str] = {
    "fc-bayern-muenchen": "FCB",
    "borussia-dortmund": "BVB",
    "rb-leipzig": "RBL",
    "bayer-04-leverkusen": "B04",
    "eintracht-frankfurt": "SGE",
    "sc-freiburg": "SCF",
    "vfb-stuttgart": "VFB",
    "vfl-wolfsburg": "WOB",
    "borussia-moenchengladbach": "BMG",
    "werder-bremen": "SVW",
    "1-fc-union-berlin": "FCU",
    "1-fc-heidenheim": "FCH",
    "1-fc-koeln": "KOE",
    "1-fc-kaiserslautern": "FCK",
    "1-fc-nuernberg": "FCN",
    "fc-augsburg": "FCA",
    "1-fsv-mainz-05": "M05",
    "vfl-bochum": "BOC",
    "tsg-hoffenheim": "TSG",
    "holstein-kiel": "KIE",
    "fc-st-pauli": "STP",
}

TEAM_CODE_ALIASES: dict[str, str] = {
    "fc bayern munchen": "FCB",
    "bayern munich": "FCB",
    "fc bayern": "FCB",
    "borussia dortmund": "BVB",
    "rb leipzig": "RBL",
    "bayer leverkusen": "B04",
    "eintracht frankfurt": "SGE",
    "sc freiburg": "SCF",
    "vfb stuttgart": "VFB",
    "vfl wolfsburg": "WOB",
    "borussia monchengladbach": "BMG",
    "borussia moenchengladbach": "BMG",
    "werder bremen": "SVW",
    "1 fc union berlin": "FCU",
    "union berlin": "FCU",
    "1 fc heidenheim": "FCH",
    "1 fc heidenheim 1846": "FCH",
    "1 fc koln": "KOE",
    "1 fc koeln": "KOE",
    "koln": "KOE",
    "koeln": "KOE",
    "fc augsburg": "FCA",
    "augsburg": "FCA",
    "1 fsv mainz 05": "M05",
    "fsv mainz 05": "M05",
    "mainz 05": "M05",
    "mainz": "M05",
    "vfl bochum": "BOC",
    "tsg hoffenheim": "TSG",
    "holstein kiel": "KIE",
    "fc st pauli": "STP",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver snapshots from bronze snapshots.")
    parser.add_argument("--lakehouse-bronze-dir", type=Path, default=Path("data/lakehouse/bronze"))
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--timestamp", type=str, default=None)
    return parser.parse_args(argv)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _first_str(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = row.get(key)
        text = _to_text(value)
        if text:
            return text
    return ""


def _normalize_ascii(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def _normalize_name(value: str) -> str:
    ascii_text = _normalize_ascii(value).lower().strip()
    ascii_text = re.sub(r"[^a-z0-9]+", " ", ascii_text)
    return " ".join(ascii_text.split())


def _slugify_name(value: str) -> str:
    lowered = _normalize_ascii(value).lower().strip()
    out: list[str] = []
    prev_dash = False
    for ch in lowered:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
            continue
        if not prev_dash:
            out.append("-")
            prev_dash = True
    return "".join(out).strip("-")


def _normalize_team_code(value: str | None) -> str | None:
    if not value:
        return None
    code = re.sub(r"[^A-Za-z0-9]+", "", value).upper()
    if not code:
        return None
    return code


def _parse_birthdate(value: Any) -> date | None:
    text = _to_text(value)
    if not text:
        return None

    match_de = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", text)
    if match_de is not None:
        day, month, year = int(match_de.group(1)), int(match_de.group(2)), int(match_de.group(3))
        try:
            return date(year, month, day)
        except ValueError:
            return None

    match_iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", text)
    if match_iso is not None:
        year, month, day = int(match_iso.group(1)), int(match_iso.group(2)), int(match_iso.group(3))
        try:
            return date(year, month, day)
        except ValueError:
            return None

    return None


def _build_player_uid(*, player_name: str, birthdate: date | None) -> str:
    name = _normalize_ascii(player_name or "")
    name = re.sub(r"[^A-Za-z0-9]+", " ", name).strip()
    if not name:
        name = "PlayerUnknown"
    tokens = [part.capitalize() for part in name.split() if part]
    compact_name = "".join(tokens) or "PlayerUnknown"
    date_token = birthdate.strftime("%Y%m%d") if birthdate is not None else "00000000"
    return f"{compact_name}{date_token}"


def _position_from_row(row: dict[str, Any]) -> str:
    value = _first_str(row, ["position", "pos"])
    if not value:
        return ""

    text = value.upper().strip()
    if text in {"GK", "DEF", "MID", "FWD"}:
        return text

    numeric = _to_int(text)
    if numeric is None:
        return text

    return {
        1: "GK",
        2: "DEF",
        3: "MID",
        4: "FWD",
    }.get(numeric, str(numeric))


def _status_from_kickbase_row(row: dict[str, Any]) -> str:
    explicit = _first_str(row, ["injury_status", "status"])
    if explicit:
        return explicit.lower()

    code = _to_int(row.get("st"))
    if code == 0:
        return "fit"
    if code == 1:
        return "questionable"
    if code == 2:
        return "injured"
    return "unknown"


def _season_uid_default(timestamp: str) -> int:
    year = _to_int(timestamp[:4])
    month = _to_int(timestamp[5:7])
    if year is None or month is None:
        return 0
    if month >= 7:
        return int(f"{year % 100:02d}{(year + 1) % 100:02d}")
    return int(f"{(year - 1) % 100:02d}{year % 100:02d}")


def _derive_competition_risk(li_row: dict[str, Any]) -> str:
    explicit = _to_text(li_row.get("competition_risk")).lower()
    if explicit and explicit not in {"unknown", "none", "nan"}:
        return explicit

    count = _to_int(li_row.get("competition_player_count"))
    if count is None and isinstance(li_row.get("competition_player_names"), list):
        count = len(li_row.get("competition_player_names", []))
    if count is None:
        return "unknown"
    if count <= 0:
        return "low"
    if count == 1:
        return "medium"
    return "high"


def _extract_ligainsider_team_slug(url: str) -> str:
    text = _to_text(url)
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
    except ValueError:
        return ""
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return ""
    return path_parts[0].strip().lower()


def _normalize_ligainsider_team_url(value: str) -> str:
    text = _to_text(value)
    if not text:
        return ""
    if "ligainsider.de" not in text:
        return text
    normalized = text
    if normalized.startswith("http://"):
        normalized = "https://" + normalized[len("http://") :]
    if not normalized.startswith("https://"):
        return normalized
    prefix = "https://www.ligainsider.de"
    if not normalized.startswith(prefix):
        return normalized
    path = normalized[len(prefix):]
    path = path.split("?", 1)[0].split("#", 1)[0]
    if "/kader/" in path:
        path = path.split("/kader/", 1)[0] + "/"
    if not path.endswith("/"):
        path += "/"
    return urlunsplit(("https", "www.ligainsider.de", path, "", ""))


def _team_code_from_ligainsider_url(value: str) -> str | None:
    slug = _extract_ligainsider_team_slug(value)
    if not slug:
        return None
    mapped = TEAM_CODE_BY_LIGAINSIDER_SLUG.get(slug)
    if mapped:
        return mapped
    fallback = _normalize_team_code(_normalize_ascii(slug.replace("-", ""))[:3])
    return fallback


def _team_code_from_odds_team_name(team_name: str) -> str | None:
    normalized_name = _normalize_name(team_name)
    if not normalized_name:
        return None
    direct = TEAM_CODE_ALIASES.get(normalized_name)
    if direct:
        return direct
    return None


def _resolve_optional_snapshot_file(
    *,
    root_dir: Path,
    dataset_name: str,
    selected_timestamp: str,
) -> tuple[Path | None, str | None]:
    dataset_dir = root_dir / dataset_name
    if not dataset_dir.exists():
        return None, None

    exact = dataset_dir / f"snapshot_{selected_timestamp}.ndjson"
    if exact.exists():
        return exact, selected_timestamp

    files = sorted(dataset_dir.glob("snapshot_*.ndjson"))
    if not files:
        return None, None

    best_path: Path | None = None
    best_ts: str | None = None
    for path in files:
        name = path.name
        if not name.startswith("snapshot_") or not name.endswith(".ndjson"):
            continue
        ts = name[len("snapshot_") : -len(".ndjson")]
        if ts <= selected_timestamp:
            best_path = path
            best_ts = ts
    if best_path is not None:
        return best_path, best_ts

    latest = files[-1]
    latest_ts = latest.name[len("snapshot_") : -len(".ndjson")]
    return latest, latest_ts


def _collect_ligainsider_indexes(
    li_status: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    by_name: dict[str, list[dict[str, Any]]] = {}
    by_slug: dict[str, list[dict[str, Any]]] = {}
    for row in li_status:
        name_key = _normalize_name(_to_text(row.get("player_name")))
        if name_key:
            by_name.setdefault(name_key, []).append(row)

        slug = _to_text(row.get("ligainsider_player_slug")).lower()
        if slug:
            by_slug.setdefault(slug, []).append(row)

    return by_name, by_slug


def _pick_ligainsider_row(
    *,
    player_name: str,
    kb_team_code: str | None,
    li_by_name: dict[str, list[dict[str, Any]]],
    li_by_slug: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    candidates = list(li_by_name.get(_normalize_name(player_name), []))
    if candidates:
        if kb_team_code:
            same_team = [
                row
                for row in candidates
                if _team_code_from_ligainsider_url(_to_text(row.get("source_url"))) == kb_team_code
            ]
            if same_team:
                candidates = same_team
        candidates.sort(
            key=lambda row: (
                _to_text(row.get("last_changed_at")),
                _to_text(row.get("scraped_at")),
            ),
            reverse=True,
        )
        return candidates[0]

    slug_guess = _slugify_name(player_name)
    slug_candidates = li_by_slug.get(slug_guess, [])
    if slug_candidates:
        return slug_candidates[0]

    return {}


def _build_player_rows(
    *,
    kb_players: list[dict[str, Any]],
    li_by_name: dict[str, list[dict[str, Any]]],
    li_by_slug: dict[str, list[dict[str, Any]]],
    selected_timestamp: str,
) -> list[dict[str, Any]]:
    snapshot_date = selected_timestamp[:10]
    season_uid = _season_uid_default(selected_timestamp)
    player_rows: dict[str, dict[str, Any]] = {}

    for row in kb_players:
        kb_player_id = _first_str(row, ["kickbase_player_id", "player_id", "id", "i"])
        if not kb_player_id:
            continue

        player_name = _first_str(row, ["player_name"])
        if not player_name:
            first_name = _first_str(row, ["first_name", "fn"])
            last_name = _first_str(row, ["last_name", "n"])
            if first_name and last_name:
                player_name = f"{first_name} {last_name}"
            else:
                player_name = first_name or last_name
        if not player_name:
            continue

        kb_team_id = _to_int(_first_str(row, ["team_id", "tid"]))
        kb_team_code = TEAM_CODE_BY_KICKBASE_TEAM_ID.get(kb_team_id or -1)

        li_row = _pick_ligainsider_row(
            player_name=player_name,
            kb_team_code=kb_team_code,
            li_by_name=li_by_name,
            li_by_slug=li_by_slug,
        )
        li_birthdate = _parse_birthdate(li_row.get("birthdate"))
        base_player_uid = _build_player_uid(player_name=player_name, birthdate=li_birthdate)
        player_uid = base_player_uid
        if player_uid in player_rows and _to_text(player_rows[player_uid].get("kb_player_id")) != kb_player_id:
            player_uid = f"{base_player_uid}_{kb_player_id}"

        li_team_code = _team_code_from_ligainsider_url(_to_text(li_row.get("source_url")))
        team_uid = kb_team_code or li_team_code
        team_name = (
            TEAM_NAME_BY_CODE.get(team_uid or "", "")
            or _first_str(row, ["team_name", "team"])
            or TEAM_NAME_BY_CODE.get(li_team_code or "", "")
        )

        competition_player_names = li_row.get("competition_player_names")
        if not isinstance(competition_player_names, list):
            competition_player_names = []
        competition_player_count = _to_int(li_row.get("competition_player_count"))
        if competition_player_count is None:
            competition_player_count = len(competition_player_names)

        player_rows[player_uid] = {
            "player_uid": player_uid,
            "player_name": player_name,
            "player_birthdate": li_birthdate.isoformat() if li_birthdate is not None else None,
            "team_uid": team_uid,
            "team_name": team_name,
            "player_position": _position_from_row(row),
            "season_uid": season_uid,
            "snapshot_date": snapshot_date,
            "source_snapshot_ts": selected_timestamp,
            "kb_player_id": kb_player_id,
            "kickbase_team_id": kb_team_id,
            "market_value": _to_int(row.get("market_value") if "market_value" in row else row.get("mv")),
            "market_value_day_change": _to_int(row.get("market_value_day_change") if "market_value_day_change" in row else row.get("mvt")),
            "market_value_history_10d": row.get("market_value_history_10d", []),
            "market_value_high_365d": _to_int(row.get("market_value_high_365d")),
            "market_value_low_365d": _to_int(row.get("market_value_low_365d")),
            "average_paid_price_10d": _to_int(row.get("average_paid_price_10d")),
            "average_points": _to_float(row.get("average_points") if "average_points" in row else row.get("ap")),
            "average_minutes": _to_float(row.get("average_minutes")),
            "appearances_total": _to_int(row.get("appearances_total")),
            "starts_total": _to_int(row.get("starts_total")),
            "goals_total": _to_int(row.get("goals_total")),
            "assists_total": _to_int(row.get("assists_total")),
            "yellow_cards_total": _to_int(row.get("yellow_cards_total")),
            "red_cards_total": _to_int(row.get("red_cards_total")),
            "total_points": _to_int(row.get("total_points") if "total_points" in row else row.get("p")),
            "last_matchday": _to_int(row.get("last_matchday")),
            "last_match_points": _to_int(row.get("last_match_points")),
            "start_probability_label": _to_text(row.get("start_probability_label")).lower(),
            "start_probability_color": _to_text(row.get("start_probability_color")),
            "injury_status": _to_text(row.get("injury_status")).lower() or _status_from_kickbase_row(row),
            "status_last_updated_at": _to_text(row.get("status_last_updated_at")),
            "ligainsider_player_id": _to_int(li_row.get("ligainsider_player_id")),
            "ligainsider_player_slug": _to_text(li_row.get("ligainsider_player_slug")),
            "ligainsider_name": _to_text(li_row.get("player_name")),
            "ligainsider_profile_url": _to_text(li_row.get("ligainsider_profile_url")),
            "ligainsider_team_url": _normalize_ligainsider_team_url(_to_text(li_row.get("source_url"))),
            "player_image_url": _to_text(li_row.get("player_image_url")),
            "li_predicted_lineup": _to_text(li_row.get("predicted_lineup")),
            "li_status": _to_text(li_row.get("status")),
            "li_competition_risk": _derive_competition_risk(li_row),
            "li_competition_player_count": competition_player_count,
            "li_competition_player_names": competition_player_names,
            "li_last_changed_at": _to_text(li_row.get("last_changed_at")),
            "li_first_seen_at": _to_text(li_row.get("first_seen_at")),
        }

    return sorted(player_rows.values(), key=lambda item: str(item.get("player_uid")))


def _init_lineup_team_row(team_uid: str, team_name: str, team_url: str, selected_timestamp: str) -> dict[str, Any]:
    return {
        "team_uid": team_uid,
        "team_name": team_name,
        "snapshot_date": selected_timestamp[:10],
        "source_snapshot_ts": selected_timestamp,
        "ligainsider_team_url": team_url,
        "lineup_last_changed_at": "",
        "safe_starter_count": 0,
        "potential_starter_count": 0,
        "bench_count": 0,
        "unknown_count": 0,
        "safe_starter_players": [],
        "potential_starter_players": [],
        "bench_players": [],
        "lineup_coverage_players": 0,
        "odds_event_id": "",
        "match_uid": "",
        "commence_time": "",
        "is_home": None,
        "opponent_team_uid": "",
        "opponent_team_name": "",
        "win_probability": None,
        "draw_probability": None,
        "loss_probability": None,
        "likely_result": "",
        "totals_line": None,
        "totals_over_probability": None,
        "totals_under_probability": None,
        "odds_collected_at": "",
        "odds_last_changed_at": "",
        "odds_snapshot_ts": "",
    }


def _append_unique_name(target: list[str], name: str) -> None:
    cleaned = _to_text(name)
    if not cleaned:
        return
    if cleaned in target:
        return
    target.append(cleaned)


def _apply_lineup_stats_from_li(
    *,
    li_status: list[dict[str, Any]],
    selected_timestamp: str,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in li_status:
        team_url = _normalize_ligainsider_team_url(_to_text(row.get("source_url")))
        team_uid = _team_code_from_ligainsider_url(team_url)
        if not team_uid:
            continue

        team_name = TEAM_NAME_BY_CODE.get(team_uid, team_uid)
        target = out.get(team_uid)
        if target is None:
            target = _init_lineup_team_row(team_uid, team_name, team_url, selected_timestamp)
            out[team_uid] = target
        elif not target.get("ligainsider_team_url") and team_url:
            target["ligainsider_team_url"] = team_url

        lineup = _to_text(row.get("predicted_lineup")).lower()
        player_name = _to_text(row.get("player_name"))
        if lineup == "safe starter":
            target["safe_starter_count"] = int(target["safe_starter_count"]) + 1
            _append_unique_name(target["safe_starter_players"], player_name)
        elif lineup == "potential starter":
            target["potential_starter_count"] = int(target["potential_starter_count"]) + 1
            _append_unique_name(target["potential_starter_players"], player_name)
        elif lineup == "bench":
            target["bench_count"] = int(target["bench_count"]) + 1
            _append_unique_name(target["bench_players"], player_name)
        else:
            target["unknown_count"] = int(target["unknown_count"]) + 1

        target["lineup_coverage_players"] = int(
            target["safe_starter_count"]
        ) + int(target["potential_starter_count"]) + int(target["bench_count"]) + int(target["unknown_count"])

        changed_at = _to_text(row.get("last_changed_at"))
        if changed_at and changed_at > _to_text(target.get("lineup_last_changed_at")):
            target["lineup_last_changed_at"] = changed_at

    return out


def _likely_result_for_team(win_prob: float | None, draw_prob: float | None, loss_prob: float | None) -> str:
    values = {
        "win": -1.0 if win_prob is None else float(win_prob),
        "draw": -1.0 if draw_prob is None else float(draw_prob),
        "loss": -1.0 if loss_prob is None else float(loss_prob),
    }
    top = max(values, key=values.get)
    return top if values[top] >= 0 else ""


def _build_match_uid_from_odds(commence_time: str, home_uid: str, away_uid: str) -> str:
    token = "unknown"
    if re.match(r"^\d{4}-\d{2}-\d{2}T", commence_time):
        token = commence_time[:10].replace("-", "")
    return f"{token}-{home_uid}{away_uid}"


def _resolve_team_name(code: str, fallback: str) -> str:
    return TEAM_NAME_BY_CODE.get(code, _to_text(fallback) or code)


def _candidate_team_row_from_odds(
    *,
    odds_row: dict[str, Any],
    selected_timestamp: str,
    odds_snapshot_ts: str,
    for_home_team: bool,
) -> dict[str, Any] | None:
    home_name = _to_text(odds_row.get("home_team"))
    away_name = _to_text(odds_row.get("away_team"))
    home_uid = _team_code_from_odds_team_name(home_name)
    away_uid = _team_code_from_odds_team_name(away_name)
    if not home_uid or not away_uid:
        return None

    commence_time = _to_text(odds_row.get("commence_time"))
    match_uid = _build_match_uid_from_odds(commence_time, home_uid, away_uid)

    if for_home_team:
        team_uid = home_uid
        team_name = _resolve_team_name(home_uid, home_name)
        opponent_uid = away_uid
        opponent_name = _resolve_team_name(away_uid, away_name)
        win_probability = odds_row.get("h2h_home_implied_prob")
        draw_probability = odds_row.get("h2h_draw_implied_prob")
        loss_probability = odds_row.get("h2h_away_implied_prob")
        is_home = True
    else:
        team_uid = away_uid
        team_name = _resolve_team_name(away_uid, away_name)
        opponent_uid = home_uid
        opponent_name = _resolve_team_name(home_uid, home_name)
        win_probability = odds_row.get("h2h_away_implied_prob")
        draw_probability = odds_row.get("h2h_draw_implied_prob")
        loss_probability = odds_row.get("h2h_home_implied_prob")
        is_home = False

    win_prob_num = _to_float(win_probability, default=-1.0)
    draw_prob_num = _to_float(draw_probability, default=-1.0)
    loss_prob_num = _to_float(loss_probability, default=-1.0)

    return {
        "team_uid": team_uid,
        "team_name": team_name,
        "snapshot_date": selected_timestamp[:10],
        "source_snapshot_ts": selected_timestamp,
        "ligainsider_team_url": "",
        "lineup_last_changed_at": "",
        "safe_starter_count": 0,
        "potential_starter_count": 0,
        "bench_count": 0,
        "unknown_count": 0,
        "safe_starter_players": [],
        "potential_starter_players": [],
        "bench_players": [],
        "lineup_coverage_players": 0,
        "odds_event_id": _to_text(odds_row.get("odds_event_id")),
        "match_uid": match_uid,
        "commence_time": commence_time,
        "is_home": is_home,
        "opponent_team_uid": opponent_uid,
        "opponent_team_name": opponent_name,
        "win_probability": win_prob_num if win_prob_num >= 0 else None,
        "draw_probability": draw_prob_num if draw_prob_num >= 0 else None,
        "loss_probability": loss_prob_num if loss_prob_num >= 0 else None,
        "likely_result": _likely_result_for_team(
            win_prob_num if win_prob_num >= 0 else None,
            draw_prob_num if draw_prob_num >= 0 else None,
            loss_prob_num if loss_prob_num >= 0 else None,
        ),
        "totals_line": odds_row.get("totals_line"),
        "totals_over_probability": odds_row.get("totals_over_implied_prob"),
        "totals_under_probability": odds_row.get("totals_under_implied_prob"),
        "odds_collected_at": _to_text(odds_row.get("odds_collected_at")),
        "odds_last_changed_at": _to_text(odds_row.get("odds_last_changed_at")),
        "odds_snapshot_ts": _to_text(odds_snapshot_ts),
    }


def _prefer_team_row(existing: dict[str, Any], candidate: dict[str, Any]) -> bool:
    existing_commence = _to_text(existing.get("commence_time"))
    candidate_commence = _to_text(candidate.get("commence_time"))

    existing_has_odds = bool(_to_text(existing.get("odds_event_id")))
    candidate_has_odds = bool(_to_text(candidate.get("odds_event_id")))
    if not existing_has_odds and candidate_has_odds:
        return True
    if existing_has_odds and not candidate_has_odds:
        return False

    if candidate_commence and not existing_commence:
        return True
    if existing_commence and not candidate_commence:
        return False
    if candidate_commence and existing_commence and candidate_commence < existing_commence:
        return True
    return False


def _merge_lineup_into_team_row(team_row: dict[str, Any], lineup_row: dict[str, Any]) -> dict[str, Any]:
    out = dict(team_row)
    if not _to_text(out.get("ligainsider_team_url")):
        out["ligainsider_team_url"] = _to_text(lineup_row.get("ligainsider_team_url"))
    out["lineup_last_changed_at"] = _to_text(lineup_row.get("lineup_last_changed_at"))
    out["safe_starter_count"] = int(lineup_row.get("safe_starter_count", 0))
    out["potential_starter_count"] = int(lineup_row.get("potential_starter_count", 0))
    out["bench_count"] = int(lineup_row.get("bench_count", 0))
    out["unknown_count"] = int(lineup_row.get("unknown_count", 0))
    out["safe_starter_players"] = list(lineup_row.get("safe_starter_players", []))
    out["potential_starter_players"] = list(lineup_row.get("potential_starter_players", []))
    out["bench_players"] = list(lineup_row.get("bench_players", []))
    out["lineup_coverage_players"] = int(lineup_row.get("lineup_coverage_players", 0))
    return out


def _build_team_rows(
    *,
    li_status: list[dict[str, Any]],
    odds_rows: list[dict[str, Any]],
    selected_timestamp: str,
    odds_snapshot_ts: str | None,
) -> list[dict[str, Any]]:
    lineup_by_team = _apply_lineup_stats_from_li(li_status=li_status, selected_timestamp=selected_timestamp)
    team_rows_by_uid: dict[str, dict[str, Any]] = {}
    odds_team_uids: set[str] = set()

    if odds_rows:
        safe_odds_snapshot_ts = _to_text(odds_snapshot_ts)
        for odds_row in odds_rows:
            for is_home in (True, False):
                candidate = _candidate_team_row_from_odds(
                    odds_row=odds_row,
                    selected_timestamp=selected_timestamp,
                    odds_snapshot_ts=safe_odds_snapshot_ts,
                    for_home_team=is_home,
                )
                if candidate is None:
                    continue
                team_uid = _to_text(candidate.get("team_uid"))
                if not team_uid:
                    continue
                odds_team_uids.add(team_uid)
                existing = team_rows_by_uid.get(team_uid)
                if existing is None or _prefer_team_row(existing, candidate):
                    team_rows_by_uid[team_uid] = candidate

    for team_uid, lineup_row in lineup_by_team.items():
        if odds_team_uids and team_uid not in odds_team_uids:
            continue
        existing = team_rows_by_uid.get(team_uid)
        if existing is None:
            existing = _init_lineup_team_row(
                team_uid=team_uid,
                team_name=_to_text(lineup_row.get("team_name")),
                team_url=_to_text(lineup_row.get("ligainsider_team_url")),
                selected_timestamp=selected_timestamp,
            )
        team_rows_by_uid[team_uid] = _merge_lineup_into_team_row(existing, lineup_row)

    out = sorted(team_rows_by_uid.values(), key=lambda item: (_to_text(item.get("team_uid"))))
    return out


def run_silver_sync(
    lakehouse_bronze_dir: Path,
    lakehouse_silver_dir: Path,
    *,
    timestamp: str | None = None,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_partitioned(
        lakehouse_bronze_dir,
        REQUIRED_INPUT_DATASETS,
    )

    input_files = find_partitioned_files_for_timestamp(
        lakehouse_bronze_dir,
        REQUIRED_INPUT_DATASETS,
        selected_timestamp,
    )

    odds_file, odds_snapshot_ts = _resolve_optional_snapshot_file(
        root_dir=lakehouse_bronze_dir,
        dataset_name=OPTIONAL_INPUT_DATASET,
        selected_timestamp=selected_timestamp,
    )

    kb_players = read_ndjson(input_files["kickbase_player_snapshot"])
    li_status = read_ndjson(input_files["ligainsider_status_snapshot"])
    odds_rows = read_ndjson(odds_file) if odds_file is not None else []

    li_by_name, li_by_slug = _collect_ligainsider_indexes(li_status)
    player_rows = _build_player_rows(
        kb_players=kb_players,
        li_by_name=li_by_name,
        li_by_slug=li_by_slug,
        selected_timestamp=selected_timestamp,
    )
    team_rows = _build_team_rows(
        li_status=li_status,
        odds_rows=odds_rows,
        selected_timestamp=selected_timestamp,
        odds_snapshot_ts=odds_snapshot_ts,
    )

    output_tables = {
        "player_snapshot": player_rows,
        "team_matchup_snapshot": team_rows,
    }

    files_written: list[str] = []
    rows_written = 0
    for table_name, rows in output_tables.items():
        output_path = lakehouse_silver_dir / table_name / f"snapshot_{selected_timestamp}.ndjson"
        write_ndjson(output_path, rows)
        files_written.append(str(output_path))
        rows_written += len(rows)

    return {
        "status": "success",
        "timestamp": selected_timestamp,
        "rows_written": rows_written,
        "tables_written": sorted(output_tables.keys()),
        "files_written": files_written,
        "odds_snapshot_ts": odds_snapshot_ts,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_silver_sync(
        args.lakehouse_bronze_dir,
        args.lakehouse_silver_dir,
        timestamp=args.timestamp,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
