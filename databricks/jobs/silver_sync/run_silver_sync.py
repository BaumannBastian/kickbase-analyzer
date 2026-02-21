# ------------------------------------
# run_silver_sync.py
#
# Dieses Skript synchronisiert Bronze-Snapshots in kanonische
# Silver-Tabellen mit stabilen Player-IDs.
#
# Outputs
# ------------------------------------
# 1) data/lakehouse/silver/dim_player/snapshot_<ts>.ndjson
# 2) data/lakehouse/silver/map_player_source/snapshot_<ts>.ndjson
# 3) data/lakehouse/silver/fct_player_daily/snapshot_<ts>.ndjson
# 4) data/lakehouse/silver/fct_player_match/snapshot_<ts>.ndjson
#
# Usage
# ------------------------------------
# - python -m databricks.jobs.silver_sync.run_silver_sync
# - python -m databricks.jobs.silver_sync.run_silver_sync --timestamp 2026-02-21T131500Z
# ------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from databricks.jobs.common_io import (
    find_partitioned_files_for_timestamp,
    latest_timestamp_common_partitioned,
    read_ndjson,
    write_ndjson,
)


INPUT_DATASETS = [
    "kickbase_player_snapshot",
    "kickbase_match_stats",
    "ligainsider_status_snapshot",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver canonical tables from bronze snapshots.")
    parser.add_argument("--lakehouse-bronze-dir", type=Path, default=Path("data/lakehouse/bronze"))
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--timestamp", type=str, default=None)
    return parser.parse_args(argv)


def _normalize_name(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _player_uid_from_kickbase_id(kickbase_player_id: str) -> str:
    return f"player_{kickbase_player_id}"


def _safe_str(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    return str(value)


def run_silver_sync(
    lakehouse_bronze_dir: Path,
    lakehouse_silver_dir: Path,
    *,
    timestamp: str | None = None,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_partitioned(
        lakehouse_bronze_dir,
        INPUT_DATASETS,
    )

    input_files = find_partitioned_files_for_timestamp(
        lakehouse_bronze_dir,
        INPUT_DATASETS,
        selected_timestamp,
    )

    kb_players = read_ndjson(input_files["kickbase_player_snapshot"])
    kb_match = read_ndjson(input_files["kickbase_match_stats"])
    li_status = read_ndjson(input_files["ligainsider_status_snapshot"])

    li_by_name: dict[str, dict[str, Any]] = {}
    for row in li_status:
        key = _normalize_name(_safe_str(row, "player_name"))
        if key:
            li_by_name[key] = row

    dim_player: list[dict[str, Any]] = []
    map_player_source: list[dict[str, Any]] = []
    fct_player_daily: list[dict[str, Any]] = []

    player_id_to_uid: dict[str, str] = {}

    snapshot_date = selected_timestamp[:10]

    for row in kb_players:
        kickbase_player_id = _safe_str(row, "kickbase_player_id")
        if not kickbase_player_id:
            continue

        player_uid = _player_uid_from_kickbase_id(kickbase_player_id)
        player_id_to_uid[kickbase_player_id] = player_uid

        player_name = _safe_str(row, "player_name")
        li_row = li_by_name.get(_normalize_name(player_name), {})

        dim_player.append(
            {
                "player_uid": player_uid,
                "canonical_name": player_name,
                "position": _safe_str(row, "position"),
                "team": _safe_str(row, "team"),
                "season": "2025/2026",
            }
        )

        map_player_source.append(
            {
                "player_uid": player_uid,
                "kickbase_player_id": kickbase_player_id,
                "ligainsider_slug": _safe_str(li_row, "ligainsider_slug"),
                "snapshot_date": snapshot_date,
            }
        )

        fct_player_daily.append(
            {
                "player_uid": player_uid,
                "snapshot_date": snapshot_date,
                "market_value": row.get("market_value"),
                "status": _safe_str(row, "status"),
                "predicted_lineup": _safe_str(li_row, "predicted_lineup"),
                "injury_note": li_row.get("injury_note"),
                "competition_risk": _safe_str(li_row, "competition_risk"),
            }
        )

    fct_player_match: list[dict[str, Any]] = []
    for row in kb_match:
        kickbase_player_id = _safe_str(row, "kickbase_player_id")
        player_uid = player_id_to_uid.get(kickbase_player_id)
        if player_uid is None:
            continue

        fct_player_match.append(
            {
                "player_uid": player_uid,
                "matchday": row.get("matchday"),
                "minutes": row.get("minutes"),
                "raw_points": row.get("raw_points"),
                "goals": row.get("goals"),
                "assists": row.get("assists"),
            }
        )

    output_tables = {
        "dim_player": dim_player,
        "map_player_source": map_player_source,
        "fct_player_daily": fct_player_daily,
        "fct_player_match": fct_player_match,
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
