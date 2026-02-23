# ------------------------------------
# check_match_points_consistency.py
#
# Dieses Skript prueft die Konsistenz zwischen
# `fact_player_match.points_total` und der Summe der Eventpunkte aus
# `fact_player_event` pro Spieler/Match.
#
# Outputs
# ------------------------------------
# 1) JSON-Summary auf stdout inkl. Mismatch-Samples.
#
# Usage
# ------------------------------------
# - python -m scripts.history.check_match_points_consistency --env-file .env
# - python -m scripts.history.check_match_points_consistency --env-file .env --player-name-like amiri
# - python -m scripts.history.check_match_points_consistency --env-file .env --limit 20
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import sys
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:  # type: ignore[override]
        return False


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prueft Match-Total vs Event-Summe in kickbase_raw.")
    parser.add_argument("--env-file", default=".env", help="Pfad zur .env Datei")
    parser.add_argument("--player-name-like", default=None, help="Optionaler Name-Filter (ILIKE)")
    parser.add_argument("--matchday", type=int, default=None, help="Optionaler Matchday-Filter")
    parser.add_argument("--limit", type=int, default=25, help="Maximale Anzahl Mismatch-Beispiele")
    return parser.parse_args(argv)


def build_matchday_like(matchday: int | None) -> str | None:
    if matchday is None:
        return None
    return f"%-MD{int(matchday):02d}-%"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)

    try:
        from src.db import DbConfig, get_connection
    except ModuleNotFoundError as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "reason": "missing_dependency",
                    "message": (
                        "PostgreSQL dependency fehlt. Bitte zuerst `python -m pip install -r requirements.txt` "
                        "oder `python -m pip install -e .` ausfuehren."
                    ),
                    "missing_module": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2

    db_config = DbConfig.from_env()
    matchday_like = build_matchday_like(args.matchday)
    player_like = f"%{args.player_name_like.strip()}%" if args.player_name_like else None

    base_params = [player_like, player_like, matchday_like, matchday_like]
    sql = """
        WITH per_match AS (
            SELECT
                m.player_uid,
                p.player_name,
                m.match_uid,
                m.points_total,
                COALESCE(SUM(e.points), 0) AS event_points_total,
                COUNT(e.event_hash) AS event_count
            FROM fact_player_match AS m
            JOIN dim_player AS p
              ON p.player_uid = m.player_uid
            LEFT JOIN fact_player_event AS e
              ON e.player_uid = m.player_uid
             AND e.match_uid = m.match_uid
            WHERE (%s IS NULL OR p.player_name ILIKE %s)
              AND (%s IS NULL OR m.match_uid LIKE %s)
            GROUP BY
                m.player_uid,
                p.player_name,
                m.match_uid,
                m.points_total
        )
        SELECT
            player_uid,
            player_name,
            match_uid,
            points_total,
            event_points_total,
            event_count,
            (points_total - event_points_total) AS delta_points
        FROM per_match
        ORDER BY player_name, match_uid
    """

    rows: list[dict[str, Any]] = []
    with get_connection(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(base_params))
            columns = [col.name for col in cur.description]
            for raw in cur.fetchall():
                rows.append(dict(zip(columns, raw, strict=False)))

    checked_total = len(rows)
    matches_with_events = [row for row in rows if int(row["event_count"]) > 0]
    matches_without_events = [row for row in rows if int(row["event_count"]) == 0]
    exact_matches = [row for row in matches_with_events if int(row["delta_points"]) == 0]
    mismatches = [row for row in matches_with_events if int(row["delta_points"]) != 0]

    mismatches_sorted = sorted(
        mismatches,
        key=lambda row: (abs(int(row["delta_points"])), str(row["player_name"]), str(row["match_uid"])),
        reverse=True,
    )
    limit = max(0, int(args.limit))
    mismatch_samples = mismatches_sorted[:limit] if limit else []

    output = {
        "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "player_name_like": args.player_name_like,
        "matchday": args.matchday,
        "checked_total": checked_total,
        "matches_with_events": len(matches_with_events),
        "matches_without_events": len(matches_without_events),
        "exact_matches": len(exact_matches),
        "mismatches": len(mismatches),
        "mismatch_samples": mismatch_samples,
    }
    print(json.dumps(output, ensure_ascii=True, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
