# ------------------------------------
# prepare_ml_exports.py
#
# Dieses Skript exportiert ML-Laufartefakte aus data/ml_runs/<run_ts>
# in BigQuery-RAW JSONL-Dateien.
#
# Outputs
# ------------------------------------
# 1) data/warehouse/raw_ml/ml_live_predictions.jsonl
# 2) data/warehouse/raw_ml/ml_cv_fold_metrics.jsonl
# 3) data/warehouse/raw_ml/ml_champion_selection.jsonl
# 4) data/warehouse/raw_ml/ml_run_summary.jsonl
# 5) data/warehouse/raw_ml/manifest.json
#
# Usage
# ------------------------------------
# - python -m bigquery.raw_load.prepare_ml_exports
# - python -m bigquery.raw_load.prepare_ml_exports --run-ts 2026-02-23T111527Z
# - python -m bigquery.raw_load.prepare_ml_exports --run-dir data/ml_runs/2026-02-23T111527Z
# ------------------------------------

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare ML JSONL exports for BigQuery RAW.")
    parser.add_argument("--ml-runs-dir", type=Path, default=Path("data/ml_runs"))
    parser.add_argument("--run-ts", type=str, default=None)
    parser.add_argument("--run-dir", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=Path("data/warehouse/raw_ml"))
    return parser.parse_args(argv)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            handle.write("\n")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _resolve_run_dir(*, ml_runs_dir: Path, run_ts: str | None, run_dir: Path | None) -> Path:
    if run_dir is not None:
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory not found: {run_dir}")
        return run_dir

    if run_ts:
        candidate = ml_runs_dir / run_ts
        if not candidate.exists():
            raise FileNotFoundError(f"Run timestamp directory not found: {candidate}")
        return candidate

    candidates = [path for path in ml_runs_dir.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No ML run directories found under {ml_runs_dir}")
    return sorted(candidates, key=lambda path: path.name)[-1]


def run_prepare_ml_exports(
    *,
    ml_runs_dir: Path,
    output_dir: Path,
    run_ts: str | None = None,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    selected_run_dir = _resolve_run_dir(ml_runs_dir=ml_runs_dir, run_ts=run_ts, run_dir=run_dir)
    selected_run_ts = selected_run_dir.name
    exported_at = now_utc_iso()

    champion_path = selected_run_dir / "champion_selection.json"
    run_summary_path = selected_run_dir / "run_summary.json"
    live_path = selected_run_dir / "live_predictions_champion.csv"

    if not run_summary_path.exists():
        raise FileNotFoundError(f"Missing run summary file: {run_summary_path}")
    if not live_path.exists():
        fallback = selected_run_dir / "live_predictions_sklearn.csv"
        if not fallback.exists():
            raise FileNotFoundError(f"Missing champion/live prediction CSV in {selected_run_dir}")
        live_path = fallback

    run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))
    champion = {}
    if champion_path.exists():
        champion = json.loads(champion_path.read_text(encoding="utf-8"))
    else:
        champion = {
            "champion_model": "sklearn",
            "champion_reason": "fallback: champion_selection.json missing",
            "metric_key": "metric_points_total_rmse",
            "sklearn_score": (
                run_summary.get("sklearn_summary", {})
                .get("metrics_mean", {})
                .get("metric_points_total_rmse")
            ),
            "torch_score": (
                run_summary.get("torch_summary", {})
                .get("metrics_mean", {})
                .get("metric_points_total_rmse")
            ),
        }

    live_rows = _read_csv_rows(live_path)
    live_export_rows: list[dict[str, Any]] = []
    for row in live_rows:
        payload = dict(row)
        payload["ml_run_ts"] = selected_run_ts
        payload["champion_model"] = champion.get("champion_model", "sklearn")
        payload["raw_exported_at"] = exported_at
        live_export_rows.append(payload)

    cv_export_rows: list[dict[str, Any]] = []
    sklearn_cv_path = selected_run_dir / "cv_sklearn_folds.csv"
    if sklearn_cv_path.exists():
        for row in _read_csv_rows(sklearn_cv_path):
            payload = dict(row)
            payload["ml_run_ts"] = selected_run_ts
            payload["model_family"] = "sklearn"
            payload["raw_exported_at"] = exported_at
            cv_export_rows.append(payload)

    torch_cv_path = selected_run_dir / "cv_torch_folds.csv"
    if torch_cv_path.exists():
        for row in _read_csv_rows(torch_cv_path):
            payload = dict(row)
            payload["ml_run_ts"] = selected_run_ts
            payload["model_family"] = "torch"
            payload["raw_exported_at"] = exported_at
            cv_export_rows.append(payload)

    champion_rows = [
        {
            "ml_run_ts": selected_run_ts,
            "champion_model": champion.get("champion_model"),
            "champion_reason": champion.get("champion_reason"),
            "metric_key": champion.get("metric_key"),
            "sklearn_score": champion.get("sklearn_score"),
            "torch_score": champion.get("torch_score"),
            "raw_exported_at": exported_at,
        }
    ]

    sklearn_metrics = run_summary.get("sklearn_summary", {}).get("metrics_mean", {})
    torch_summary_payload = run_summary.get("torch_summary")
    if isinstance(torch_summary_payload, dict):
        torch_metrics = torch_summary_payload.get("metrics_mean", {})
    else:
        torch_metrics = {}
    run_rows = [
        {
            "ml_run_ts": selected_run_ts,
            "history_players": run_summary.get("history_players"),
            "history_rows": run_summary.get("history_rows"),
            "live_rows": run_summary.get("live_rows"),
            "champion_model": champion.get("champion_model"),
            "sklearn_points_total_rmse": sklearn_metrics.get("metric_points_total_rmse"),
            "torch_points_total_rmse": torch_metrics.get("metric_points_total_rmse"),
            "run_summary": run_summary,
            "raw_exported_at": exported_at,
        }
    ]

    files_written: list[str] = []

    live_out = output_dir / "ml_live_predictions.jsonl"
    _write_jsonl(live_out, live_export_rows)
    files_written.append(str(live_out))

    cv_out = output_dir / "ml_cv_fold_metrics.jsonl"
    _write_jsonl(cv_out, cv_export_rows)
    files_written.append(str(cv_out))

    champion_out = output_dir / "ml_champion_selection.jsonl"
    _write_jsonl(champion_out, champion_rows)
    files_written.append(str(champion_out))

    run_summary_out = output_dir / "ml_run_summary.jsonl"
    _write_jsonl(run_summary_out, run_rows)
    files_written.append(str(run_summary_out))

    manifest = {
        "generated_at": exported_at,
        "source_ml_run_ts": selected_run_ts,
        "source_run_dir": str(selected_run_dir),
        "tables": {
            "ml_live_predictions": len(live_export_rows),
            "ml_cv_fold_metrics": len(cv_export_rows),
            "ml_champion_selection": len(champion_rows),
            "ml_run_summary": len(run_rows),
        },
        "files": files_written,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")
    files_written.append(str(manifest_path))

    return {
        "status": "success",
        "run_ts": selected_run_ts,
        "source_run_dir": str(selected_run_dir),
        "files_written": files_written,
        "row_count_by_table": manifest["tables"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_prepare_ml_exports(
        ml_runs_dir=args.ml_runs_dir,
        output_dir=args.output_dir,
        run_ts=args.run_ts,
        run_dir=args.run_dir,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
