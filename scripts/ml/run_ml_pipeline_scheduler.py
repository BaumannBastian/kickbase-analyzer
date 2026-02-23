# ------------------------------------
# run_ml_pipeline_scheduler.py
#
# Dieses Skript startet die ML->BigQuery Pipeline in fixem Intervall.
# Es eignet sich fuer lokale Scheduler-Runs (Task Scheduler/Cron),
# wenn historische CV-Runs regelmaessig automatisiert laufen sollen.
#
# Outputs
# ------------------------------------
# 1) Wiederholte ML-Run-Artefakte in data/ml_runs
# 2) Wiederholte RAW-ML Exporte in data/warehouse/raw_ml
# 3) Optionaler BigQuery-RAW Upload pro Lauf
#
# Usage
# ------------------------------------
# - python -m scripts.ml.run_ml_pipeline_scheduler --interval-seconds 21600 -- --env-file .env --project kickbase-analyzer
# - python -m scripts.ml.run_ml_pipeline_scheduler --max-runs 1 -- --env-file .env --skip-bq-load
# ------------------------------------

from __future__ import annotations

import argparse
import json
import time
from typing import Sequence

from scripts.ml.run_ml_bigquery_pipeline import main as run_pipeline_main


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ML->BigQuery pipeline repeatedly.")
    parser.add_argument("--interval-seconds", type=float, default=21600.0)
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("pipeline_args", nargs=argparse.REMAINDER)
    return parser.parse_args(argv)


def run_scheduler(
    *,
    pipeline_args: list[str],
    interval_seconds: float,
    max_runs: int,
) -> int:
    if interval_seconds < 0:
        raise ValueError("interval_seconds must be >= 0")
    if max_runs < 0:
        raise ValueError("max_runs must be >= 0")

    cleaned_args = list(pipeline_args)
    if cleaned_args and cleaned_args[0] == "--":
        cleaned_args = cleaned_args[1:]

    runs = 0
    while True:
        exit_code = run_pipeline_main(cleaned_args)
        if exit_code != 0:
            raise RuntimeError(f"Pipeline exited with code {exit_code}")

        runs += 1
        print(json.dumps({"status": "success", "run": runs}, ensure_ascii=True, sort_keys=True))

        if max_runs > 0 and runs >= max_runs:
            break
        time.sleep(interval_seconds)

    return runs


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    run_scheduler(
        pipeline_args=list(args.pipeline_args),
        interval_seconds=args.interval_seconds,
        max_runs=args.max_runs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
