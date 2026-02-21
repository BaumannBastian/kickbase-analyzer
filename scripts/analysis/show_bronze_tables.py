# ------------------------------------
# show_bronze_tables.py
#
# Dieses Skript zeigt die Bronze-Tabellen (Kickbase, LigaInsider
# und optional Wettquoten) in einer DataFrame-aehnlichen Terminalansicht.
# Wenn pandas verfuegbar ist, wird pandas fuer die Darstellung
# genutzt; sonst wird ein eingebauter Tabellenrenderer verwendet.
#
# Outputs
# ------------------------------------
# 1) Keine Dateiausgabe; schreibt formatierte Tabellen auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.analysis.show_bronze_tables
# - python -m scripts.analysis.show_bronze_tables --input-dir data/bronze_preview --limit 25
# - python -m scripts.analysis.show_bronze_tables --timestamp 2026-02-21T170722Z
# - python -m scripts.analysis.show_bronze_tables --input-dir data/bronze --player Orban
# ------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence


KB_FILE_PREFIX = "kickbase_player_snapshot_"
LI_FILE_PREFIX = "ligainsider_status_snapshot_"
ODDS_FILE_PREFIX = "odds_match_snapshot_"


KB_COLUMNS = [
    "kickbase_player_id",
    "player_name",
    "team_id",
    "position",
    "market_value",
    "average_points",
    "average_minutes",
    "appearances_total",
    "starts_total",
    "goals_total",
    "assists_total",
    "yellow_cards_total",
    "red_cards_total",
    "start_probability_label",
    "injury_status",
    "status_last_updated_at",
]


LI_COLUMNS = [
    "ligainsider_player_slug",
    "ligainsider_player_id",
    "player_name",
    "predicted_lineup",
    "competition_player_count",
    "competition_player_names",
    "status",
    "last_changed_at",
]


ODDS_COLUMNS = [
    "odds_event_id",
    "commence_time",
    "odds_collected_at",
    "odds_last_changed_at",
    "home_team",
    "away_team",
    "h2h_home_odds",
    "h2h_draw_odds",
    "h2h_away_odds",
    "totals_line",
    "totals_over_odds",
    "totals_under_odds",
    "totals_available_line_count",
    "bookmaker_count",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show Kickbase and LigaInsider bronze rows as tables.")
    parser.add_argument("--input-dir", type=Path, default=Path("data/bronze_preview"))
    parser.add_argument("--fallback-input-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--timestamp", type=str, default=None)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument(
        "--player",
        type=str,
        default=None,
        help="Optional substring filter on player_name for both tables.",
    )
    return parser.parse_args(argv)


def _resolve_input_dir(preferred: Path, fallback: Path) -> Path:
    if preferred.exists():
        return preferred
    return fallback


def _latest_timestamp_for_prefix(input_dir: Path, prefix: str) -> str | None:
    files = sorted(input_dir.glob(f"{prefix}*.ndjson"))
    if not files:
        return None
    return files[-1].stem.replace(prefix, "")


def _resolve_table_path(
    input_dir: Path,
    *,
    prefix: str,
    requested_timestamp: str | None,
) -> tuple[Path | None, str | None]:
    if requested_timestamp:
        requested_path = input_dir / f"{prefix}{requested_timestamp}.ndjson"
        if requested_path.exists():
            return requested_path, requested_timestamp

    latest_ts = _latest_timestamp_for_prefix(input_dir, prefix)
    if latest_ts is None:
        return None, None
    return input_dir / f"{prefix}{latest_ts}.ndjson", latest_ts


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _filter_rows_by_player(rows: list[dict[str, Any]], player_filter: str | None) -> list[dict[str, Any]]:
    if player_filter is None or not player_filter.strip():
        return rows
    needle = player_filter.strip().lower()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        searchable_fields = [
            str(row.get("player_name", "")).strip().lower(),
            str(row.get("home_team", "")).strip().lower(),
            str(row.get("away_team", "")).strip().lower(),
        ]
        if any(needle in field for field in searchable_fields if field):
            filtered.append(row)
    return filtered


def _truncate(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def _format_scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _render_table(rows: list[dict[str, Any]], columns: list[str], limit: int) -> str:
    sliced = rows[: max(0, limit)]
    if not sliced:
        return "(no rows)"

    formatted_rows: list[list[str]] = []
    widths = [len(col) for col in columns]

    for row in sliced:
        line: list[str] = []
        for idx, col in enumerate(columns):
            text = _format_scalar(row.get(col))
            text = _truncate(text, 60)
            line.append(text)
            widths[idx] = max(widths[idx], len(text))
        formatted_rows.append(line)

    header = " | ".join(col.ljust(widths[idx]) for idx, col in enumerate(columns))
    separator = "-+-".join("-" * widths[idx] for idx in range(len(columns)))
    body = [
        " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(line))
        for line in formatted_rows
    ]
    return "\n".join([header, separator, *body])


def _try_render_with_pandas(rows: list[dict[str, Any]], columns: list[str], limit: int) -> str | None:
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError:
        return None

    frame = pd.DataFrame(rows)
    for col in columns:
        if col not in frame.columns:
            frame[col] = None
    frame = frame.loc[:, columns].head(max(0, limit))
    return frame.to_string(index=False, max_colwidth=60)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    input_dir = _resolve_input_dir(args.input_dir, args.fallback_input_dir)

    kb_path, kb_timestamp = _resolve_table_path(
        input_dir,
        prefix=KB_FILE_PREFIX,
        requested_timestamp=args.timestamp,
    )
    li_path, li_timestamp = _resolve_table_path(
        input_dir,
        prefix=LI_FILE_PREFIX,
        requested_timestamp=args.timestamp,
    )
    odds_path, odds_timestamp = _resolve_table_path(
        input_dir,
        prefix=ODDS_FILE_PREFIX,
        requested_timestamp=args.timestamp,
    )

    if kb_path is None or li_path is None:
        raise FileNotFoundError(
            f"Missing required Kickbase/LigaInsider bronze files in {input_dir}"
        )

    kb_rows = _read_ndjson(kb_path)
    li_rows = _read_ndjson(li_path)
    odds_rows = _read_ndjson(odds_path) if odds_path is not None else []
    kb_rows = _filter_rows_by_player(kb_rows, args.player)
    li_rows = _filter_rows_by_player(li_rows, args.player)
    odds_rows = _filter_rows_by_player(odds_rows, args.player)

    kb_render = _try_render_with_pandas(kb_rows, KB_COLUMNS, args.limit)
    if kb_render is None:
        kb_render = _render_table(kb_rows, KB_COLUMNS, args.limit)

    li_render = _try_render_with_pandas(li_rows, LI_COLUMNS, args.limit)
    if li_render is None:
        li_render = _render_table(li_rows, LI_COLUMNS, args.limit)

    odds_render = _try_render_with_pandas(odds_rows, ODDS_COLUMNS, args.limit)
    if odds_render is None:
        odds_render = _render_table(odds_rows, ODDS_COLUMNS, args.limit)

    print("Bronze table timestamps:")
    print(f"- Kickbase: {kb_timestamp}")
    print(f"- LigaInsider: {li_timestamp}")
    print(f"- Odds: {odds_timestamp or 'missing'}")
    print(f"Input directory: {input_dir}")
    if args.player:
        print(f"Player filter: {args.player}")
    print("")
    print(f"Kickbase rows: {len(kb_rows)} (showing {min(len(kb_rows), args.limit)})")
    print(kb_render)
    print("")
    print(f"LigaInsider rows: {len(li_rows)} (showing {min(len(li_rows), args.limit)})")
    print(li_render)
    print("")
    print(f"Odds rows: {len(odds_rows)} (showing {min(len(odds_rows), args.limit)})")
    print(odds_render)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
