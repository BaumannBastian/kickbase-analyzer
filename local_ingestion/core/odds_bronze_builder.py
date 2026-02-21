# ------------------------------------
# odds_bronze_builder.py
#
# Dieses Modul normalisiert Events aus The Odds API in ein
# Bronze-Row-Format fuer kommende Bundesliga-Spiele.
#
# Outputs
# ------------------------------------
# 1) List[dict] fuer `odds_match_snapshot` Bronze-Output.
#
# Usage
# ------------------------------------
# - rows = build_odds_rows(events)
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
from statistics import median
from typing import Any


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_iso(value: Any) -> str | None:
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
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _median_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 4)


def _normalize_probability(*odds_values: float | None) -> list[float | None]:
    implied: list[float | None] = []
    for odds in odds_values:
        if odds is None or odds <= 0:
            implied.append(None)
            continue
        implied.append(1.0 / odds)

    total = sum(value for value in implied if value is not None)
    if total <= 0:
        return implied

    normalized: list[float | None] = []
    for value in implied:
        if value is None:
            normalized.append(None)
        else:
            normalized.append(round(value / total, 6))
    return normalized


def build_odds_rows(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for event in events:
        if not isinstance(event, dict):
            continue

        event_id = str(event.get("id", "")).strip()
        sport_key = str(event.get("sport_key", "")).strip()
        sport_title = str(event.get("sport_title", "")).strip()
        commence_time = _to_iso(event.get("commence_time"))
        home_team = str(event.get("home_team", "")).strip()
        away_team = str(event.get("away_team", "")).strip()

        home_prices: list[float] = []
        draw_prices: list[float] = []
        away_prices: list[float] = []

        totals_by_point: dict[float, dict[str, list[float]]] = {}

        bookmakers = event.get("bookmakers")
        bookmaker_keys: list[str] = []
        if isinstance(bookmakers, list):
            for bookmaker in bookmakers:
                if not isinstance(bookmaker, dict):
                    continue
                key = str(bookmaker.get("key", "")).strip()
                if key:
                    bookmaker_keys.append(key)

                markets = bookmaker.get("markets")
                if not isinstance(markets, list):
                    continue

                for market in markets:
                    if not isinstance(market, dict):
                        continue
                    market_key = str(market.get("key", "")).strip().lower()
                    outcomes = market.get("outcomes")
                    if not isinstance(outcomes, list):
                        continue

                    if market_key == "h2h":
                        for outcome in outcomes:
                            if not isinstance(outcome, dict):
                                continue
                            name = str(outcome.get("name", "")).strip()
                            price = _to_float(outcome.get("price"))
                            if price is None or price <= 0:
                                continue
                            if home_team and name == home_team:
                                home_prices.append(price)
                            elif away_team and name == away_team:
                                away_prices.append(price)
                            elif name.lower() == "draw":
                                draw_prices.append(price)

                    if market_key == "totals":
                        for outcome in outcomes:
                            if not isinstance(outcome, dict):
                                continue
                            name = str(outcome.get("name", "")).strip().lower()
                            if name not in {"over", "under"}:
                                continue
                            price = _to_float(outcome.get("price"))
                            if price is None or price <= 0:
                                continue
                            point = _to_float(outcome.get("point"))
                            if point is None:
                                point = _to_float(market.get("point"))
                            if point is None:
                                continue
                            slot = totals_by_point.setdefault(point, {"over": [], "under": []})
                            slot[name].append(price)

        h2h_home_odds = _median_or_none(home_prices)
        h2h_draw_odds = _median_or_none(draw_prices)
        h2h_away_odds = _median_or_none(away_prices)
        h2h_probs = _normalize_probability(h2h_home_odds, h2h_draw_odds, h2h_away_odds)

        totals_line: float | None = None
        totals_over_odds: float | None = None
        totals_under_odds: float | None = None

        if totals_by_point:
            candidate_points = sorted(
                totals_by_point.items(),
                key=lambda item: (
                    abs(item[0] - 2.5),
                    -(len(item[1]["over"]) + len(item[1]["under"])),
                ),
            )
            selected_point, payload = candidate_points[0]
            totals_line = round(float(selected_point), 2)
            totals_over_odds = _median_or_none(payload["over"])
            totals_under_odds = _median_or_none(payload["under"])

        totals_probs = _normalize_probability(totals_over_odds, totals_under_odds)

        rows.append(
            {
                "odds_event_id": event_id,
                "sport_key": sport_key,
                "sport_title": sport_title,
                "commence_time": commence_time,
                "home_team": home_team,
                "away_team": away_team,
                "bookmaker_count": len(bookmaker_keys),
                "bookmaker_keys": bookmaker_keys,
                "h2h_home_odds": h2h_home_odds,
                "h2h_draw_odds": h2h_draw_odds,
                "h2h_away_odds": h2h_away_odds,
                "h2h_home_implied_prob": h2h_probs[0],
                "h2h_draw_implied_prob": h2h_probs[1],
                "h2h_away_implied_prob": h2h_probs[2],
                "totals_line": totals_line,
                "totals_over_odds": totals_over_odds,
                "totals_under_odds": totals_under_odds,
                "totals_over_implied_prob": totals_probs[0],
                "totals_under_implied_prob": totals_probs[1],
                "raw_event": event,
            }
        )

    rows.sort(key=lambda row: str(row.get("commence_time") or ""))
    return rows
