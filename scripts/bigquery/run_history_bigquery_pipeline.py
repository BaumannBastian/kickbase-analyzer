# ------------------------------------
# run_history_bigquery_pipeline.py
#
# Dieses Skript fuehrt den selektiven History-BigQuery-Flow end-to-end aus:
# 1) Export aus Postgres + Bronze in RAW JSONL
# 2) Upload der RAW JSONL nach BigQuery
# 3) Apply der History CORE/MARTS Views
#
# Usage
# ------------------------------------
# - python -m scripts.bigquery.run_history_bigquery_pipeline --env-file .env --project kickbase-analyzer
# - python -m scripts.bigquery.run_history_bigquery_pipeline --env-file .env --project kickbase-analyzer --skip-live-snapshots
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Any, Sequence

from bigquery.core_transform.apply_history_views_with_bq_cli import main as apply_history_views_main
from bigquery.raw_load.load_history_with_bq_cli import run_load_history_with_bq_cli
from bigquery.raw_load.prepare_history_exports import run_prepare_history_exports

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:  # type: ignore[override]
        return False


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run selective history export/load/views pipeline for BigQuery.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))

    parser.add_argument("--output-dir", type=Path, default=Path("data/warehouse/raw_history"))
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--skip-live-snapshots", action="store_true")

    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="kickbase_raw")
    parser.add_argument("--location", type=str, default="EU")
    parser.add_argument("--write-disposition", choices=["append", "truncate"], default="append")
    parser.add_argument("--skip-bq-load", action="store_true")
    parser.add_argument("--skip-history-views", action="store_true")
    parser.add_argument("--dry-run-bq", action="store_true")
    return parser.parse_args(argv)


def run_history_bigquery_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    load_dotenv(args.env_file)

    prepare_summary = run_prepare_history_exports(
        output_dir=args.output_dir,
        bronze_dir=args.bronze_dir,
        include_live_snapshots=not args.skip_live_snapshots,
    )

    project = args.project or os.environ.get("BQ_PROJECT_ID", "").strip()
    if not project and not args.skip_bq_load:
        raise ValueError("Missing BQ project. Use --project or BQ_PROJECT_ID.")

    bq_summary: dict[str, Any]
    views_summary: dict[str, Any]

    if args.skip_bq_load:
        bq_summary = {
            "status": "skipped",
            "reason": "skip_bq_load=true",
        }
        views_summary = {
            "status": "skipped",
            "reason": "skip_bq_load=true",
        }
    else:
        bq_summary = run_load_history_with_bq_cli(
            project=project,
            dataset=args.dataset,
            location=args.location,
            input_dir=args.output_dir,
            write_disposition=args.write_disposition,
            dry_run=args.dry_run_bq,
        )

        if args.skip_history_views:
            views_summary = {
                "status": "skipped",
                "reason": "skip_history_views=true",
            }
        else:
            apply_args = [
                "--env-file",
                str(args.env_file),
                "--project",
                project,
                "--raw",
                args.dataset,
                "--core",
                "kickbase_core",
                "--marts",
                "kickbase_marts",
                "--location",
                args.location,
            ]
            if args.dry_run_bq:
                apply_args.append("--dry-run")
            exit_code = apply_history_views_main(apply_args)
            if exit_code != 0:
                raise RuntimeError(f"History view apply failed with exit code {exit_code}")
            views_summary = {
                "status": "success",
                "project": project,
                "core_dataset": "kickbase_core",
                "marts_dataset": "kickbase_marts",
            }

    return {
        "status": "success",
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "prepare_history_exports": prepare_summary,
        "bq_load": bq_summary,
        "history_views": views_summary,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_history_bigquery_pipeline(args)
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
