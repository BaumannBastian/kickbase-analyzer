# ------------------------------------
# ligainsider_bronze_builder.py
#
# Dieses Modul reichert LigaInsider-Rohzeilen um stabile Bronze-
# Felder an: Lineup-Flag, Konkurrenz-Felder und Change-Tracking.
#
# Outputs
# ------------------------------------
# 1) List[dict] fuer `ligainsider_status_snapshot` Bronze-Output.
#
# Usage
# ------------------------------------
# - rows = build_ligainsider_rows(raw_rows, kb_rows, previous_rows)
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from typing import Any
import unicodedata
from urllib.parse import urlparse


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_text.lower().strip().split())


def _lineup_flag(value: str) -> bool:
    text = (value or "").strip().lower()
    return text in {"starter", "startelf", "starting11", "starting", "true", "1"}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_name_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _safe_text(item)
        if not text:
            continue
        norm = _normalize_name(text)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(text)
    return out


def _to_iso_utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _row_key(row: dict[str, Any]) -> str:
    slug = str(row.get("ligainsider_player_slug", "")).strip().lower()
    if slug:
        return f"slug:{slug}"
    name = _normalize_name(str(row.get("player_name", "")))
    return f"name:{name}"


def _competition_risk_label(*, is_in_lineup: bool, competitor_count: int) -> str:
    if competitor_count <= 0:
        return "low"
    if not is_in_lineup:
        return "high"
    if competitor_count >= 2:
        return "medium"
    return "low"


def _team_slug_from_source_url(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    return parts[0].strip().lower()


def _fingerprint_row(row: dict[str, Any]) -> str:
    payload = {
        "predicted_lineup": row.get("predicted_lineup"),
        "status": row.get("status"),
        "competition_player_names": row.get("competition_player_names"),
        "has_position_competition": row.get("has_position_competition"),
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_ligainsider_rows(
    *,
    raw_rows: list[dict[str, Any]],
    kickbase_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    kb_by_name: dict[str, dict[str, Any]] = {}
    for kb_row in kickbase_rows:
        key = _normalize_name(str(kb_row.get("player_name", "")))
        if not key:
            continue
        # Bei Duplikaten gewinnt der Spieler mit hoeherem Marktwert.
        current = kb_by_name.get(key)
        if current is None:
            kb_by_name[key] = kb_row
            continue
        current_mv = float(current.get("market_value") or 0.0)
        new_mv = float(kb_row.get("market_value") or 0.0)
        if new_mv > current_mv:
            kb_by_name[key] = kb_row

    out_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        player_name = str(row.get("player_name", "")).strip()
        normalized_name = _normalize_name(player_name)
        kb_row = kb_by_name.get(normalized_name)

        predicted_lineup = str(row.get("predicted_lineup", "")).strip().lower()
        status = str(row.get("status", "")).strip().lower()
        if not status or status == "unknown":
            status = str((kb_row or {}).get("injury_status", "unknown")).strip().lower() or "unknown"

        out: dict[str, Any] = dict(row)
        out["predicted_lineup"] = predicted_lineup or "unknown"
        out["is_in_lineup"] = _lineup_flag(predicted_lineup)
        out["status"] = status
        out["kickbase_player_id"] = (kb_row or {}).get("kickbase_player_id")
        out["kickbase_team_id"] = (kb_row or {}).get("team_id")
        out["kickbase_position_code"] = (kb_row or {}).get("position_code")
        out["kickbase_position"] = (kb_row or {}).get("position")
        out["ligainsider_team_slug"] = _team_slug_from_source_url(row.get("source_url"))
        out_rows.append(out)

    grouped_by_position: dict[tuple[str, str], list[dict[str, Any]]] = {}
    grouped_by_team: dict[str, list[dict[str, Any]]] = {}
    for row in out_rows:
        team_id = _safe_text(row.get("kickbase_team_id"))
        position = _safe_text(row.get("kickbase_position"))
        if not team_id or not position:
            team_slug = _safe_text(row.get("ligainsider_team_slug"))
            if team_slug:
                grouped_by_team.setdefault(team_slug, []).append(row)
            continue

        grouped_by_position.setdefault((team_id, position), []).append(row)

        team_slug = _safe_text(row.get("ligainsider_team_slug"))
        if team_slug:
            grouped_by_team.setdefault(team_slug, []).append(row)

    for row in out_rows:
        team_id = _safe_text(row.get("kickbase_team_id"))
        position = _safe_text(row.get("kickbase_position"))
        team_slug = _safe_text(row.get("ligainsider_team_slug"))
        row_name_norm = _normalize_name(_safe_text(row.get("player_name")))

        explicit_competitors = [
            name
            for name in _safe_name_list(row.get("competition_player_names"))
            if _normalize_name(name) != row_name_norm
        ]

        if isinstance(row.get("competition_player_names"), list):
            row["competition_player_names"] = explicit_competitors
            row["has_position_competition"] = len(explicit_competitors) > 0
            row["competition_player_count"] = len(explicit_competitors)
            row["competition_scope"] = "ligainsider_column"
            row["competition_risk"] = _competition_risk_label(
                is_in_lineup=bool(row.get("is_in_lineup")),
                competitor_count=len(explicit_competitors),
            )
            continue

        peers: list[dict[str, Any]] = []
        competition_scope = "none"
        if team_id and position:
            peers = grouped_by_position.get((team_id, position), [])
            competition_scope = "team_position"
        elif team_slug:
            peers = grouped_by_team.get(team_slug, [])
            competition_scope = "team_fallback"

        competitors = [
            str(peer.get("player_name", "")).strip()
            for peer in peers
            if _row_key(peer) != _row_key(row)
        ]
        competitors = sorted(name for name in competitors if name)

        row["competition_player_names"] = competitors
        row["has_position_competition"] = len(competitors) > 0
        row["competition_player_count"] = len(competitors)
        row["competition_scope"] = competition_scope
        row["competition_risk"] = _competition_risk_label(
            is_in_lineup=bool(row.get("is_in_lineup")),
            competitor_count=len(competitors),
        )

    previous_by_key = {_row_key(row): row for row in previous_rows}
    now_iso = _to_iso_utc_now()
    for row in out_rows:
        key = _row_key(row)
        previous = previous_by_key.get(key, {})
        row_fp = _fingerprint_row(row)
        previous_fp = str(previous.get("change_fingerprint", "")).strip()

        if previous and previous_fp == row_fp:
            row["last_changed_at"] = previous.get("last_changed_at") or previous.get("scraped_at") or now_iso
        else:
            row["last_changed_at"] = row.get("scraped_at") or now_iso

        row["first_seen_at"] = previous.get("first_seen_at") or row.get("scraped_at") or now_iso
        row["change_fingerprint"] = row_fp

    return out_rows
