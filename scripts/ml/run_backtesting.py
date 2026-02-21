# ------------------------------------
# run_backtesting.py
#
# Dieses Skript fuehrt ein matchday-ausgerichtetes Backtesting der
# Gold-Praediktionen durch. Es matched Vorhersagen gegen tatsaechliche
# Rohpunkte und exportiert zusammenfassende Kennzahlen.
#
# Outputs
# ------------------------------------
# 1) data/backtesting/backtest_summary_<timestamp>.csv
# 2) data/backtesting/backtest_errors_<timestamp>.csv
#
# Usage
# ------------------------------------
# - python -m scripts.ml.run_backtesting
# - python -m scripts.ml.run_backtesting --lakehouse-gold-dir data/lakehouse/gold --lakehouse-silver-dir data/lakehouse/silver
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import statistics
from typing import Any, Sequence

from databricks.jobs.common_io import read_ndjson, write_csv


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local backtesting for expected points predictions.")
    parser.add_argument("--lakehouse-gold-dir", type=Path, default=Path("data/lakehouse/gold"))
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/backtesting"))
    return parser.parse_args(argv)


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _collect_latest_rows_by_key(
    dataset_dir: Path,
    key_fields: tuple[str, ...],
) -> dict[tuple[str, ...], dict[str, Any]]:
    files = sorted(dataset_dir.glob("snapshot_*.ndjson"))
    if not files:
        raise FileNotFoundError(f"No snapshot files found under {dataset_dir}")

    latest_by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    for path in files:
        rows = read_ndjson(path)
        for row in rows:
            key = tuple(str(row.get(field, "")).strip() for field in key_fields)
            if any(part == "" for part in key):
                continue
            latest_by_key[key] = row
    return latest_by_key


def _compute_metric_rows(error_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not error_rows:
        return []

    def summarize(scope: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        residuals = [_to_float(row.get("residual_points"), 0.0) for row in rows]
        abs_residuals = [abs(value) for value in residuals]
        sq_residuals = [value * value for value in residuals]
        return {
            "scope": scope,
            "sample_size": len(rows),
            "mae": round(statistics.fmean(abs_residuals), 4),
            "rmse": round(math.sqrt(statistics.fmean(sq_residuals)), 4),
            "bias": round(statistics.fmean(residuals), 4),
        }

    metrics: list[dict[str, Any]] = []
    metrics.append(summarize("overall", error_rows))

    rows_by_position: dict[str, list[dict[str, Any]]] = {}
    for row in error_rows:
        position = str(row.get("position", "")).strip() or "UNKNOWN"
        rows_by_position.setdefault(position, []).append(row)

    for position in sorted(rows_by_position.keys()):
        metrics.append(summarize(f"position:{position}", rows_by_position[position]))

    return metrics


def run_backtesting(
    lakehouse_gold_dir: Path,
    lakehouse_silver_dir: Path,
    out_dir: Path,
) -> dict[str, Any]:
    pred_dir = lakehouse_gold_dir / "feat_player_matchday"
    actual_dir = lakehouse_silver_dir / "fct_player_match"

    pred_rows = _collect_latest_rows_by_key(
        pred_dir,
        key_fields=("player_uid", "matchday"),
    )
    actual_rows = _collect_latest_rows_by_key(
        actual_dir,
        key_fields=("player_uid", "matchday"),
    )

    errors: list[dict[str, Any]] = []
    for key, pred in pred_rows.items():
        actual = actual_rows.get(key)
        if actual is None:
            continue

        pred_points = _to_float(pred.get("expected_points_next_matchday"), 0.0)
        actual_points = _to_float(actual.get("raw_points"), 0.0)

        errors.append(
            {
                "player_uid": pred.get("player_uid"),
                "player_name": pred.get("player_name"),
                "position": pred.get("position"),
                "matchday": _to_int(pred.get("matchday"), 0),
                "predicted_points": round(pred_points, 4),
                "actual_points": round(actual_points, 4),
                "residual_points": round(pred_points - actual_points, 4),
                "abs_error_points": round(abs(pred_points - actual_points), 4),
            }
        )

    if not errors:
        raise RuntimeError("Backtesting produced no matched prediction/actual rows.")

    metric_rows = _compute_metric_rows(errors)
    exported_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%SZ")

    summary_path = out_dir / f"backtest_summary_{exported_ts}.csv"
    errors_path = out_dir / f"backtest_errors_{exported_ts}.csv"

    write_csv(
        summary_path,
        metric_rows,
        fieldnames=["scope", "sample_size", "mae", "rmse", "bias"],
    )
    write_csv(
        errors_path,
        sorted(errors, key=lambda row: float(row.get("abs_error_points", 0.0)), reverse=True),
        fieldnames=[
            "player_uid",
            "player_name",
            "position",
            "matchday",
            "predicted_points",
            "actual_points",
            "residual_points",
            "abs_error_points",
        ],
    )

    return {
        "status": "success",
        "matched_rows": len(errors),
        "summary_rows": len(metric_rows),
        "files_written": [str(summary_path), str(errors_path)],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_backtesting(
        args.lakehouse_gold_dir,
        args.lakehouse_silver_dir,
        args.out_dir,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
