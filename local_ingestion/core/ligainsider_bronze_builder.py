# ------------------------------------
# ligainsider_bronze_builder.py
#
# Dieses Modul normalisiert LigaInsider-Rohzeilen fuer den Bronze-
# Output ohne Cross-Source-Join.
#
# Outputs
# ------------------------------------
# 1) List[dict] fuer `ligainsider_status_snapshot` Bronze-Output.
#
# Usage
# ------------------------------------
# - rows = build_ligainsider_rows(raw_rows, previous_rows)
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from typing import Any
import unicodedata


def _normalize_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_text.lower().strip().split())


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


def _normalize_predicted_lineup(*, raw_lineup: str, competitor_count: int) -> str:
    text = (raw_lineup or "").strip().lower()
    if text in {"bench", "bank", "reserve", "0", "false"}:
        return "Bench"
    if text in {"starter", "startelf", "starting11", "starting", "true", "1"}:
        if competitor_count > 0:
            return "Potential Starter"
        return "Safe Starter"
    return "Unknown"


def _fingerprint_row(row: dict[str, Any]) -> str:
    payload = {
        "predicted_lineup": row.get("predicted_lineup"),
        "status": row.get("status"),
        "competition_player_names": row.get("competition_player_names"),
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_ligainsider_rows(
    *,
    raw_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        status = str(row.get("status", "")).strip().lower() or "unknown"
        competitors = _safe_name_list(row.get("competition_player_names"))
        row_name_norm = _normalize_name(_safe_text(row.get("player_name")))
        competitors = [
            name
            for name in competitors
            if _normalize_name(name) != row_name_norm
        ]
        competitor_count = len(competitors)

        out: dict[str, Any] = dict(row)
        out["predicted_lineup"] = _normalize_predicted_lineup(
            raw_lineup=_safe_text(row.get("predicted_lineup")),
            competitor_count=competitor_count,
        )
        out["status"] = status
        out["competition_player_names"] = competitors
        out["competition_player_count"] = competitor_count
        out_rows.append(out)

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
