# ------------------------------------
# build_marts_local.py
#
# Dieses Skript erzeugt lokale MARTS-Tabellen aus Gold-Daten.
# Die Outputs sind direkt bewertbar und koennen spaeter als
# Grundlage fuer BigQuery/Power BI genutzt werden.
#
# Outputs
# ------------------------------------
# 1) data/marts/mart_player_leaderboard_<ts>.csv
# 2) data/marts/mart_points_breakdown_<ts>.csv
# 3) data/marts/mart_risk_overview_<ts>.csv
#
# Usage
# ------------------------------------
# - python -m bigquery.marts.build_marts_local
# - python -m bigquery.marts.build_marts_local --timestamp 2026-02-21T131500Z
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
    write_csv,
)


INPUT_DATASETS = [
    "feat_player_daily",
    "points_components_matchday",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local MARTS tables from gold snapshots.")
    parser.add_argument("--lakehouse-gold-dir", type=Path, default=Path("data/lakehouse/gold"))
    parser.add_argument("--marts-dir", type=Path, default=Path("data/marts"))
    parser.add_argument("--timestamp", type=str, default=None)
    return parser.parse_args(argv)


def run_build_marts_local(
    lakehouse_gold_dir: Path,
    marts_dir: Path,
    *,
    timestamp: str | None = None,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_partitioned(
        lakehouse_gold_dir,
        INPUT_DATASETS,
    )

    input_files = find_partitioned_files_for_timestamp(
        lakehouse_gold_dir,
        INPUT_DATASETS,
        selected_timestamp,
    )

    feat_daily = read_ndjson(input_files["feat_player_daily"])
    points_breakdown = read_ndjson(input_files["points_components_matchday"])

    leaderboard = sorted(
        feat_daily,
        key=lambda row: float(row.get("value_score", 0.0)),
        reverse=True,
    )

    risk_overview: list[dict[str, Any]] = []
    for row in feat_daily:
        risk_overview.append(
            {
                "player_uid": row.get("player_uid"),
                "player_name": row.get("player_name"),
                "p_dnp": row.get("p_dnp"),
                "stddev_points": row.get("stddev_points"),
                "p10_points": row.get("p10_points"),
                "p50_points": row.get("p50_points"),
                "p90_points": row.get("p90_points"),
            }
        )

    leaderboard_path = marts_dir / f"mart_player_leaderboard_{selected_timestamp}.csv"
    points_path = marts_dir / f"mart_points_breakdown_{selected_timestamp}.csv"
    risk_path = marts_dir / f"mart_risk_overview_{selected_timestamp}.csv"

    write_csv(
        leaderboard_path,
        leaderboard,
        fieldnames=[
            "player_uid",
            "player_name",
            "team",
            "position",
            "start_probability",
            "expected_points_next_matchday",
            "market_value",
            "expected_marketvalue_next_matchday",
            "expected_marketvalue_change_7d",
            "value_score",
        ],
    )

    write_csv(
        points_path,
        points_breakdown,
        fieldnames=[
            "player_uid",
            "player_name",
            "matchday",
            "base_raw_ev",
            "scorer_ev",
            "win_ev",
            "minutes_bonus_ev",
            "cards_negative_ev",
            "pred_total",
        ],
    )

    write_csv(
        risk_path,
        risk_overview,
        fieldnames=[
            "player_uid",
            "player_name",
            "p_dnp",
            "stddev_points",
            "p10_points",
            "p50_points",
            "p90_points",
        ],
    )

    preview = leaderboard[0] if leaderboard else {}

    return {
        "status": "success",
        "timestamp": selected_timestamp,
        "files_written": [str(leaderboard_path), str(points_path), str(risk_path)],
        "top_player_preview": preview,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_build_marts_local(
        args.lakehouse_gold_dir,
        args.marts_dir,
        timestamp=args.timestamp,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
