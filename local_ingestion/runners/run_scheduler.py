# ------------------------------------
# run_scheduler.py
#
# Dieses Skript fuehrt Ingestion-Runs in festem Intervall aus.
# Es kann fuer Demo- oder Private-Mode genutzt werden und eignet sich
# fuer lokale Dauerausfuehrung.
#
# Outputs
# ------------------------------------
# 1) Wiederholte Bronze-Outputs je Lauf
# 2) Laufzusammenfassung auf stdout (JSON)
#
# Usage
# ------------------------------------
# - python -m local_ingestion.runners.run_scheduler --mode private --interval-seconds 1800
# - python -m local_ingestion.runners.run_scheduler --mode demo --max-runs 1
# ------------------------------------

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import time
from typing import Callable, Sequence

from local_ingestion.core.bronze_writer import run_demo_ingestion
from local_ingestion.core.config import PrivateIngestionConfig, load_private_ingestion_config
from local_ingestion.core.private_ingestion import run_private_ingestion


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ingestion repeatedly with fixed interval.")
    parser.add_argument("--mode", choices=["demo", "private"], default="private")
    parser.add_argument("--interval-seconds", type=float, default=3600.0)
    parser.add_argument("--max-runs", type=int, default=0)
    parser.add_argument("--demo-dir", type=Path, default=Path("demo/data"))
    parser.add_argument("--out-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--source-version", type=str, default=None)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args(argv)


def run_scheduler(
    *,
    run_once: Callable[[], dict[str, object]],
    interval_seconds: float,
    max_runs: int,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    if interval_seconds < 0:
        raise ValueError("interval_seconds must be >= 0")
    if max_runs < 0:
        raise ValueError("max_runs must be >= 0")

    runs = 0
    while True:
        summary = run_once()
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
        runs += 1

        if max_runs > 0 and runs >= max_runs:
            break
        sleep_fn(interval_seconds)

    return runs


def _build_private_runner(
    *,
    env_file: Path,
    out_dir: Path,
    source_version: str | None,
) -> Callable[[], dict[str, object]]:
    config: PrivateIngestionConfig = load_private_ingestion_config(env_file)
    if source_version:
        config = replace(config, source_version=source_version)

    def _run_once() -> dict[str, object]:
        return run_private_ingestion(config, out_dir)

    return _run_once


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.mode == "demo":
        source_version = args.source_version or "demo-v1"

        def run_once() -> dict[str, object]:
            return run_demo_ingestion(args.demo_dir, args.out_dir, source_version=source_version)

    else:
        run_once = _build_private_runner(
            env_file=args.env_file,
            out_dir=args.out_dir,
            source_version=args.source_version,
        )

    run_scheduler(
        run_once=run_once,
        interval_seconds=args.interval_seconds,
        max_runs=args.max_runs,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
