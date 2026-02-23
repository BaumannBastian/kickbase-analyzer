# ------------------------------------
# run_history_batches.py
#
# Dieses Skript fuehrt `src.etl_history` in kontrollierten Batches
# aus (Offset + Max-Players), um API-Last und Laufzeit zu steuern.
#
# Outputs
# ------------------------------------
# 1) Konsolidiertes JSON-Run-Summary pro Batch auf stdout.
#
# Usage
# ------------------------------------
# - python -m scripts.history.run_history_batches --players-csv in/players_all.csv --batch-size 50
# - python -m scripts.history.run_history_batches --players-csv in/players_all.csv --start-offset 100 --batch-size 50
# ------------------------------------

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import json
import subprocess
import sys
from typing import Any


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fuehrt History-ETL in Batches aus.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--players-csv", required=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--end-offset", type=int, default=None)
    parser.add_argument("--competition-id", type=int, default=1)
    parser.add_argument("--timeframe-days", type=int, default=3650)
    parser.add_argument("--days-from", type=int, default=1)
    parser.add_argument("--days-to", type=int, default=None)
    parser.add_argument("--rps", type=float, default=2.0)
    parser.add_argument("--skip-image-download", action="store_true")
    return parser.parse_args(argv)


def _count_csv_rows(path: str) -> int:
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def _run_single_batch(
    *,
    env_file: str,
    players_csv: str,
    offset: int,
    batch_size: int,
    competition_id: int,
    timeframe_days: int,
    days_from: int,
    days_to: int | None,
    rps: float,
    skip_image_download: bool,
) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-m",
        "src.etl_history",
        "--env-file",
        env_file,
        "--players-csv",
        players_csv,
        "--player-offset",
        str(offset),
        "--max-players",
        str(batch_size),
        "--competition-id",
        str(competition_id),
        "--timeframe-days",
        str(timeframe_days),
        "--days-from",
        str(days_from),
        "--rps",
        str(rps),
    ]
    if days_to is not None:
        cmd.extend(["--days-to", str(days_to)])
    if skip_image_download:
        cmd.append("--skip-image-download")

    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"Batch offset={offset} fehlgeschlagen (exit={proc.returncode}).\n"
            f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )

    payload = json.loads(proc.stdout.strip())
    payload["offset"] = offset
    payload["batch_size"] = batch_size
    return payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.batch_size <= 0:
        raise ValueError("--batch-size muss > 0 sein")

    total_rows = _count_csv_rows(args.players_csv)
    start_offset = max(0, int(args.start_offset))
    end_offset = int(args.end_offset) if args.end_offset is not None else total_rows
    end_offset = min(end_offset, total_rows)

    batch_summaries: list[dict[str, Any]] = []
    offset = start_offset
    while offset < end_offset:
        summary = _run_single_batch(
            env_file=args.env_file,
            players_csv=args.players_csv,
            offset=offset,
            batch_size=args.batch_size,
            competition_id=args.competition_id,
            timeframe_days=args.timeframe_days,
            days_from=args.days_from,
            days_to=args.days_to,
            rps=args.rps,
            skip_image_download=bool(args.skip_image_download),
        )
        batch_summaries.append(summary)
        processed = int(summary.get("players_processed", 0))
        if processed <= 0:
            break
        offset += processed

    output = {
        "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "players_csv": args.players_csv,
        "total_rows_in_csv": total_rows,
        "range_start_offset": start_offset,
        "range_end_offset": end_offset,
        "batch_size": args.batch_size,
        "batches_run": len(batch_summaries),
        "total_players_processed": sum(int(item.get("players_processed", 0)) for item in batch_summaries),
        "batch_summaries": batch_summaries,
    }
    print(json.dumps(output, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
