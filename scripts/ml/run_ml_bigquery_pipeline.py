# ------------------------------------
# run_ml_bigquery_pipeline.py
#
# Dieses Skript fuehrt die lokale ML-Pipeline end-to-end aus:
# 1) CV/Training + Live-Predictions
# 2) Export der ML-Run-Artefakte als BigQuery-RAW JSONL
# 3) Upload dieser ML-RAW Dateien nach BigQuery
#
# Outputs
# ------------------------------------
# 1) data/ml_runs/<run_ts>/*
# 2) data/warehouse/raw_ml/*.jsonl
# 3) BigQuery RAW Tabellen (ml_*)
#
# Usage
# ------------------------------------
# - python -m scripts.ml.run_ml_bigquery_pipeline --env-file .env --project kickbase-analyzer
# - python -m scripts.ml.run_ml_bigquery_pipeline --env-file .env --skip-torch --skip-bq-load
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Any, Sequence

from bigquery.core_transform.apply_ml_views_with_bq_cli import main as apply_ml_views_main
from bigquery.raw_load.load_ml_with_bq_cli import run_load_ml_with_bq_cli
from bigquery.raw_load.prepare_ml_exports import run_prepare_ml_exports
from scripts.ml.train_hierarchical_models import main as train_main

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:  # type: ignore[override]
        return False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ML training + BigQuery RAW export/load pipeline.")

    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--lakehouse-silver-dir", type=Path, default=Path("data/lakehouse/silver"))
    parser.add_argument("--silver-timestamp", default=None)
    parser.add_argument("--out-dir", type=Path, default=Path("data/ml_runs"))
    parser.add_argument("--season-window", type=int, default=3)
    parser.add_argument("--history-max-players", type=int, default=0)
    parser.add_argument("--cv-splits", type=int, default=4)
    parser.add_argument("--min-train-days", type=int, default=60)
    parser.add_argument("--validation-days", type=int, default=21)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--skip-torch", action="store_true")
    parser.add_argument("--torch-epochs", type=int, default=120)
    parser.add_argument("--torch-batch-size", type=int, default=256)
    parser.add_argument("--torch-device", default="cpu")
    parser.add_argument("--live-limit", type=int, default=0)

    parser.add_argument("--ml-raw-output-dir", type=Path, default=Path("data/warehouse/raw_ml"))
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="kickbase_raw")
    parser.add_argument("--location", type=str, default="EU")
    parser.add_argument("--write-disposition", choices=["append", "truncate"], default="append")
    parser.add_argument("--skip-bq-load", action="store_true")
    parser.add_argument("--dry-run-bq", action="store_true")
    parser.add_argument("--skip-ml-views", action="store_true")
    return parser.parse_args(argv)


def _latest_run_dir(out_dir: Path) -> Path | None:
    if not out_dir.exists():
        return None
    candidates = [path for path in out_dir.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: path.name)[-1]


def _build_train_argv(args: argparse.Namespace) -> list[str]:
    train_argv: list[str] = [
        "--env-file", str(args.env_file),
        "--lakehouse-silver-dir", str(args.lakehouse_silver_dir),
        "--out-dir", str(args.out_dir),
        "--season-window", str(args.season_window),
        "--history-max-players", str(args.history_max_players),
        "--cv-splits", str(args.cv_splits),
        "--min-train-days", str(args.min_train_days),
        "--validation-days", str(args.validation_days),
        "--random-state", str(args.random_state),
        "--torch-epochs", str(args.torch_epochs),
        "--torch-batch-size", str(args.torch_batch_size),
        "--torch-device", str(args.torch_device),
        "--live-limit", str(args.live_limit),
    ]

    if args.silver_timestamp:
        train_argv.extend(["--silver-timestamp", str(args.silver_timestamp)])
    if args.skip_torch:
        train_argv.append("--skip-torch")

    return train_argv


def run_ml_bigquery_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    load_dotenv(args.env_file)

    before_run_dir = _latest_run_dir(args.out_dir)
    before_name = before_run_dir.name if before_run_dir is not None else None
    train_exit_code = train_main(_build_train_argv(args))
    if train_exit_code != 0:
        raise RuntimeError(f"ML training failed with exit code {train_exit_code}")

    after_run_dir = _latest_run_dir(args.out_dir)
    if after_run_dir is None:
        raise FileNotFoundError(f"No ML run directory found under {args.out_dir}")
    after_name = after_run_dir.name

    if before_name == after_name:
        # Ein Run kann bei identischem Timestamp-Namen theoretisch dieselbe Dir nutzen.
        # Dann wird diese dennoch als Source genommen.
        selected_run_dir = after_run_dir
    else:
        selected_run_dir = after_run_dir

    prepare_summary = run_prepare_ml_exports(
        ml_runs_dir=args.out_dir,
        output_dir=args.ml_raw_output_dir,
        run_dir=selected_run_dir,
    )

    bq_summary: dict[str, Any] | None = None
    ml_views_summary: dict[str, Any] | None = None
    if args.skip_bq_load:
        bq_summary = {
            "status": "skipped",
            "reason": "skip_bq_load=true",
        }
        ml_views_summary = {
            "status": "skipped",
            "reason": "skip_bq_load=true",
        }
    else:
        project = args.project or os.environ.get("BQ_PROJECT_ID", "").strip()
        if not project:
            raise ValueError("Missing BQ project. Use --project or BQ_PROJECT_ID.")
        bq_summary = run_load_ml_with_bq_cli(
            project=project,
            dataset=args.dataset,
            location=args.location,
            input_dir=args.ml_raw_output_dir,
            write_disposition=args.write_disposition,
            dry_run=args.dry_run_bq,
        )
        if args.skip_ml_views:
            ml_views_summary = {
                "status": "skipped",
                "reason": "skip_ml_views=true",
            }
        else:
            apply_args = [
                "--project", project,
                "--raw", args.dataset,
                "--core", "kickbase_core",
                "--marts", "kickbase_marts",
                "--location", args.location,
            ]
            if args.dry_run_bq:
                apply_args.append("--dry-run")
            apply_exit_code = apply_ml_views_main(apply_args)
            if apply_exit_code != 0:
                raise RuntimeError(f"ML view apply failed with exit code {apply_exit_code}")
            ml_views_summary = {
                "status": "success",
                "project": project,
                "core_dataset": "kickbase_core",
                "marts_dataset": "kickbase_marts",
            }

    return {
        "status": "success",
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "ml_run_dir": str(selected_run_dir),
        "prepare_ml_exports": prepare_summary,
        "bq_load": bq_summary,
        "ml_views": ml_views_summary,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_ml_bigquery_pipeline(args)
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
