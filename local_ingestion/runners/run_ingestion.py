# ------------------------------------
# run_ingestion.py
#
# Dieses Skript startet den Ingestion-Flow im Demo- oder Private-Mode.
# Im Demo-Mode werden statische Beispieldaten verarbeitet. Im
# Private-Mode werden Kickbase Snapshots via API geholt.
#
# Outputs
# ------------------------------------
# 1) data/bronze/<dataset>_<timestamp>.ndjson
# 2) data/bronze/ingestion_runs.ndjson
#
# Usage
# ------------------------------------
# - python -m local_ingestion.runners.run_ingestion --mode demo
# - python -m local_ingestion.runners.run_ingestion --mode private --env-file .env
# ------------------------------------

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
from typing import Sequence

from local_ingestion.core.bronze_writer import TIMESTAMP_FILE_FORMAT, run_demo_ingestion
from local_ingestion.core.config import load_private_ingestion_config
from local_ingestion.core.private_ingestion import run_private_ingestion


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local ingestion in demo or private mode.")
    parser.add_argument(
        "--mode",
        choices=["demo", "private"],
        default="demo",
        help="Ingestion mode.",
    )
    parser.add_argument(
        "--demo-dir",
        type=Path,
        default=Path("demo/data"),
        help="Directory containing demo snapshot JSON files.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/bronze"),
        help="Output directory for bronze NDJSON files.",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help=f"Optional fixed UTC timestamp in format {TIMESTAMP_FILE_FORMAT}.",
    )
    parser.add_argument(
        "--source-version",
        type=str,
        default=None,
        help="Optional source version override.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Path to .env for private mode.",
    )
    return parser.parse_args(argv)


def parse_timestamp(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    return datetime.strptime(raw, TIMESTAMP_FILE_FORMAT)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    timestamp = parse_timestamp(args.timestamp)

    if args.mode == "demo":
        source_version = args.source_version or "demo-v1"
        summary = run_demo_ingestion(
            args.demo_dir,
            args.out_dir,
            now=timestamp,
            source_version=source_version,
        )
    else:
        config = load_private_ingestion_config(args.env_file)
        if args.source_version:
            config = replace(config, source_version=args.source_version)
        summary = run_private_ingestion(config, args.out_dir, now=timestamp)

    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
