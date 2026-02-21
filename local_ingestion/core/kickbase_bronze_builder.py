# ------------------------------------
# kickbase_bronze_builder.py
#
# Dieses Modul transformiert Kickbase-Rohpayloads in ein
# einheitliches Bronze-Row-Format pro Spieler.
# Es kombiniert Markt-Snapshot, Spieler-Details, Performance,
# Marktwert-Historie und Transferdaten.
#
# Outputs
# ------------------------------------
# 1) List[dict] fuer `kickbase_player_snapshot` Bronze-Output.
#
# Usage
# ------------------------------------
# - row = build_kickbase_player_row(...)
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


START_PROBABILITY_MAP: dict[int, tuple[str, str]] = {
    1: ("blue", "sicher"),
    2: ("green", "starter"),
    3: ("orange", "wahrscheinlich"),
    4: ("red", "unwahrscheinlich"),
    5: ("cross", "ausgeschlossen"),
}


INJURY_STATUS_MAP: dict[int, tuple[str, bool]] = {
    0: ("fit", False),
    1: ("questionable", False),
    2: ("injured", True),
    3: ("suspended", True),
    4: ("out", True),
}


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_iso_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    if isinstance(value, (int, float)):
        epoch = float(value)
        if epoch > 10_000_000_000:
            epoch = epoch / 1000.0
        try:
            return datetime.fromtimestamp(epoch, tz=UTC)
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

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _first_number(node: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        if key not in node:
            continue
        num = _to_float(node.get(key))
        if num is not None:
            return num
    return None


def _first_text(node: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list)):
            text = str(value).strip()
            if text:
                return text
    return ""


def _position_label(code: int | None) -> str:
    if code is None:
        return ""
    return {
        1: "GK",
        2: "DEF",
        3: "MID",
        4: "FWD",
    }.get(code, str(code))


def _extract_history_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if isinstance(payload, dict):
        for key in ("it", "data", "items", "history", "rows", "result"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [row for row in candidate if isinstance(row, dict)]
    return []


def _extract_market_value_points(history_payload: Any) -> list[tuple[datetime, float]]:
    points: list[tuple[datetime, float]] = []
    rows = _extract_history_rows(history_payload)

    for row in rows:
        date_value = None
        for key in ("d", "dt", "date", "timestamp", "ts", "t"):
            if key in row:
                date_value = row.get(key)
                break
        dt = _parse_datetime(date_value)
        if dt is None:
            continue

        mv = _first_number(row, ["mv", "market_value", "value", "v"])
        if mv is None:
            continue

        points.append((dt, mv))

    points.sort(key=lambda item: item[0])
    return points


def _extract_transfer_events(transfers_payload: Any) -> list[tuple[datetime, float]]:
    events: list[tuple[datetime, float]] = []
    rows = _extract_history_rows(transfers_payload)

    for row in rows:
        date_value = None
        for key in ("d", "dt", "date", "timestamp", "ts", "created_at"):
            if key in row:
                date_value = row.get(key)
                break
        dt = _parse_datetime(date_value)
        if dt is None:
            continue

        price = _first_number(
            row,
            [
                "prc",
                "price",
                "paid_price",
                "transfer_price",
                "amount",
                "mv",
            ],
        )
        if price is None:
            continue

        # Transferpreise unter 100k sind in der Praxis i.d.R. keine Marktwerte.
        if price < 100_000:
            continue

        events.append((dt, price))

    events.sort(key=lambda item: item[0])
    return events


def _extract_performance_match_rows(performance_payload: Any) -> list[dict[str, Any]]:
    if isinstance(performance_payload, dict):
        for key in ("ph", "history", "matches", "items", "data", "it"):
            candidate = performance_payload.get(key)
            if isinstance(candidate, list):
                return [row for row in candidate if isinstance(row, dict)]

    if isinstance(performance_payload, list):
        return [row for row in performance_payload if isinstance(row, dict)]

    return []


def _extract_playing_stats(
    details_payload: dict[str, Any],
    performance_payload: Any,
) -> dict[str, int | float | None]:
    match_rows = _extract_performance_match_rows(performance_payload)

    appearances_hist = 0
    starts_hist = 0
    total_minutes_hist = 0.0
    last_matchday: int | None = None
    last_match_points: float | None = None

    for row in match_rows:
        played = False
        hp = row.get("hp")
        if isinstance(hp, bool):
            played = hp

        minutes = _first_number(row, ["minutes", "mins", "m", "min"])
        if minutes is None:
            seconds = _first_number(row, ["sec", "seconds"])
            if seconds is not None:
                minutes = seconds / 60.0
        if minutes is not None and minutes > 0:
            played = True
            total_minutes_hist += minutes

        points = _first_number(row, ["p", "points", "raw_points"])
        if points is not None:
            played = True

        matchday = _to_int(_first_number(row, ["matchday", "md", "mday", "spieltag"]))
        if matchday is not None and (last_matchday is None or matchday >= last_matchday):
            last_matchday = matchday
            if points is not None:
                last_match_points = points

        if played:
            appearances_hist += 1

        started_raw = row.get("st")
        started = False
        if isinstance(started_raw, bool):
            started = started_raw
        elif isinstance(started_raw, (int, float)):
            started = float(started_raw) > 0
        elif isinstance(started_raw, str):
            started = started_raw.strip().lower() in {"1", "true", "starter", "startelf"}

        if started:
            starts_hist += 1

    # Kickbase liefert typischerweise die beiden Counter `smc`/`ismc`.
    # Beobachtung in Live-Daten: `starts <= appearances`.
    # Deshalb wird das Paar defensiv ueber min/max aufgeloest.
    smc_count = _to_int(_first_number(details_payload, ["smc"]))
    ismc_count = _to_int(_first_number(details_payload, ["ismc"]))

    appearances_details = _to_int(
        _first_number(details_payload, ["appearances", "apps", "matches", "games"])
    )
    starts_details = _to_int(
        _first_number(details_payload, ["starts", "s11", "starting_eleven", "startelf"])
    )

    if smc_count is not None and ismc_count is not None:
        derived_starts = min(smc_count, ismc_count)
        derived_appearances = max(smc_count, ismc_count)
    else:
        derived_starts = smc_count
        derived_appearances = ismc_count

    if starts_details is None:
        starts_details = derived_starts
    if appearances_details is None:
        appearances_details = derived_appearances

    total_minutes = _first_number(
        details_payload,
        ["total_minutes", "minutes_total", "mins", "minutes", "tm"],
    )
    if total_minutes is None:
        total_seconds = _first_number(details_payload, ["sec", "seconds", "seconds_played"])
        if total_seconds is not None:
            total_minutes = total_seconds / 60.0

    appearances = appearances_details
    if appearances is None and appearances_hist > 0:
        appearances = appearances_hist

    starts = starts_details
    if starts is None and starts_hist > 0:
        starts = starts_hist

    avg_minutes = _first_number(
        details_payload,
        ["avg_minutes", "average_minutes", "minutes_avg", "avg_min"],
    )
    if avg_minutes is None:
        if total_minutes is None and total_minutes_hist > 0:
            total_minutes = total_minutes_hist
        if appearances and appearances > 0 and total_minutes is not None:
            avg_minutes = total_minutes / float(appearances)

    if last_match_points is None:
        for row in reversed(match_rows):
            points = _first_number(row, ["p", "points", "raw_points"])
            if points is not None:
                last_match_points = points
                break

    return {
        "appearances_total": appearances,
        "starts_total": starts,
        "average_minutes": avg_minutes,
        "last_matchday": last_matchday,
        "last_match_points": last_match_points,
    }


def build_kickbase_player_row(
    *,
    market_row: dict[str, Any],
    details_payload: dict[str, Any],
    market_value_history_payload: Any,
    performance_payload: Any,
    transfers_payload: Any,
    snapshot_ts: datetime,
) -> dict[str, Any]:
    player_id = _first_text(market_row, ["kickbase_player_id", "player_id", "id", "i", "pi"])
    if not player_id:
        player_id = _first_text(details_payload, ["kickbase_player_id", "player_id", "id", "i", "pi"])

    first_name = _first_text(market_row, ["first_name", "fn"])
    if not first_name:
        first_name = _first_text(details_payload, ["first_name", "fn"])

    last_name = _first_text(market_row, ["last_name", "ln"])
    if not last_name:
        last_name = _first_text(details_payload, ["last_name", "ln", "n"])
    if not last_name:
        last_name = _first_text(market_row, ["n"])

    player_name = _first_text(market_row, ["player_name", "name", "full_name"])
    if not player_name:
        player_name = _first_text(details_payload, ["player_name", "name"])
    if not player_name:
        player_name = " ".join(part for part in [first_name, last_name] if part).strip()
    if not player_name:
        player_name = _first_text(market_row, ["n"])

    team_id = _first_text(market_row, ["team_id", "tid"])
    if not team_id:
        team_id = _first_text(details_payload, ["team_id", "tid"])

    position_code = _to_int(_first_number(market_row, ["position", "pos"]))
    if position_code is None:
        position_code = _to_int(_first_number(details_payload, ["position", "pos"]))

    market_value = _to_int(_first_number(market_row, ["market_value", "mv"]))
    if market_value is None:
        market_value = _to_int(_first_number(details_payload, ["market_value", "mv", "cv"]))

    market_value_day_change = _to_int(_first_number(market_row, ["mvt", "market_value_change_1d"]))
    if market_value_day_change is None:
        market_value_day_change = _to_int(_first_number(details_payload, ["mvt", "tfhmvt"]))

    points = _to_int(_first_number(market_row, ["points", "p", "tp"]))
    if points is None:
        points = _to_int(_first_number(details_payload, ["points", "p", "tp"]))

    average_points = _to_float(_first_number(market_row, ["average_points", "ap"]))
    if average_points is None:
        average_points = _to_float(_first_number(details_payload, ["average_points", "ap"]))

    status_code = _to_int(_first_number(market_row, ["st", "status_code"]))
    if status_code is None:
        status_code = _to_int(_first_number(details_payload, ["st", "status_code"]))

    status_last_updated_at = _first_text(market_row, ["dt", "status_updated_at", "updated_at"])
    if not status_last_updated_at:
        status_last_updated_at = _first_text(details_payload, ["dt", "ts", "status_updated_at", "updated_at"])
    status_lookup = status_code if status_code is not None else -999
    status_label, is_injured = INJURY_STATUS_MAP.get(status_lookup, ("unknown", False))

    prob_code = _to_int(_first_number(market_row, ["prob", "start_probability_code"]))
    if prob_code is None:
        prob_code = _to_int(_first_number(details_payload, ["prob", "start_probability_code"]))
    prob_lookup = prob_code if prob_code is not None else -999
    prob_color, prob_label = START_PROBABILITY_MAP.get(prob_lookup, ("unknown", "unknown"))

    stats = _extract_playing_stats(details_payload, performance_payload)

    goals_total = _to_int(_first_number(details_payload, ["g", "goals", "goals_total"]))
    assists_total = _to_int(_first_number(details_payload, ["a", "assists", "assists_total"]))
    yellow_cards_total = _to_int(_first_number(details_payload, ["y", "yellow_cards", "yellow_total"]))
    red_cards_total = _to_int(_first_number(details_payload, ["r", "red_cards", "red_total"]))

    mv_points = _extract_market_value_points(market_value_history_payload)
    ten_day_start = snapshot_ts - timedelta(days=10)
    one_year_start = snapshot_ts - timedelta(days=365)

    mv_10d = [point for point in mv_points if point[0] >= ten_day_start]
    mv_365d = [point for point in mv_points if point[0] >= one_year_start]
    if not mv_365d:
        mv_365d = mv_points

    market_value_history_10d = [
        {
            "date": _to_iso_utc(dt),
            "market_value": int(round(value)),
        }
        for dt, value in mv_10d
    ]

    market_value_high_365d = int(round(max((value for _, value in mv_365d), default=float(market_value or 0))))
    market_value_low_365d = int(round(min((value for _, value in mv_365d), default=float(market_value or 0))))

    transfer_events = _extract_transfer_events(transfers_payload)
    transfer_10d = [event for event in transfer_events if event[0] >= ten_day_start]
    avg_paid_price_10d: float | None = None
    if transfer_10d:
        avg_paid_price_10d = sum(price for _, price in transfer_10d) / len(transfer_10d)

    out: dict[str, Any] = {
        "kickbase_player_id": player_id,
        "player_name": player_name,
        "first_name": first_name,
        "last_name": last_name,
        "team_id": team_id,
        "position_code": position_code,
        "position": _position_label(position_code),
        "market_value": market_value,
        "market_value_day_change": market_value_day_change,
        "market_value_history_10d": market_value_history_10d,
        "market_value_high_365d": market_value_high_365d,
        "market_value_low_365d": market_value_low_365d,
        "average_paid_price_10d": round(avg_paid_price_10d, 2) if avg_paid_price_10d is not None else None,
        "average_points": average_points,
        "total_points": points,
        "average_minutes": round(float(stats["average_minutes"]), 2)
        if stats["average_minutes"] is not None
        else None,
        "last_matchday": stats["last_matchday"],
        "last_match_points": round(float(stats["last_match_points"]), 2)
        if stats["last_match_points"] is not None
        else None,
        "appearances_total": stats["appearances_total"],
        "starts_total": stats["starts_total"],
        "goals_total": goals_total,
        "assists_total": assists_total,
        "yellow_cards_total": yellow_cards_total,
        "red_cards_total": red_cards_total,
        "start_probability_code": prob_code,
        "start_probability_color": prob_color,
        "start_probability_label": prob_label,
        "injury_status_code": status_code,
        "injury_status": status_label,
        "is_injured": is_injured,
        "status_last_updated_at": status_last_updated_at or None,
        "raw_market_row": market_row,
        "raw_player_details": details_payload or None,
        "raw_player_performance": performance_payload or None,
        "raw_player_transfers": _extract_history_rows(transfers_payload),
    }
    return out
