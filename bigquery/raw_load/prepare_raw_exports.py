# ------------------------------------
# prepare_raw_exports.py
#
# Dieses Skript exportiert Gold-Snapshots in reproduzierbare
# BigQuery-RAW JSONL-Dateien inklusive Manifest.
#
# Outputs
# ------------------------------------
# 1) data/warehouse/raw/feat_player_daily.jsonl
# 2) data/warehouse/raw/points_components_matchday.jsonl
# 3) data/warehouse/raw/quality_metrics.jsonl
# 4) data/warehouse/raw/manifest.json
#
# Usage
# ------------------------------------
# - python -m bigquery.raw_load.prepare_raw_exports
# - python -m bigquery.raw_load.prepare_raw_exports --timestamp 2026-02-21T133000Z
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Sequence

from databricks.jobs.common_io import (
    find_partitioned_files_for_timestamp,
    latest_timestamp_common_partitioned,
    read_ndjson,
)


INPUT_TABLES = [
    "feat_player_daily",
    "points_components_matchday",
    "quality_metrics",
]


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            f.write("\n")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare BigQuery RAW JSONL exports from gold snapshots.")
    parser.add_argument("--lakehouse-gold-dir", type=Path, default=Path("data/lakehouse/gold"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/warehouse/raw"))
    parser.add_argument("--timestamp", type=str, default=None)
    return parser.parse_args(argv)


def run_prepare_raw_exports(
    lakehouse_gold_dir: Path,
    output_dir: Path,
    *,
    timestamp: str | None = None,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_partitioned(
        lakehouse_gold_dir,
        INPUT_TABLES,
    )

    files = find_partitioned_files_for_timestamp(lakehouse_gold_dir, INPUT_TABLES, selected_timestamp)

    files_written: list[str] = []
    row_count_by_table: dict[str, int] = {}

    for table_name in INPUT_TABLES:
        rows = read_ndjson(files[table_name])
        export_rows: list[dict[str, Any]] = []
        for row in rows:
            enriched = dict(row)
            enriched["raw_exported_at"] = now_utc_iso()
            enriched["source_snapshot_ts"] = selected_timestamp
            export_rows.append(enriched)

        out_path = output_dir / f"{table_name}.jsonl"
        write_jsonl(out_path, export_rows)
        files_written.append(str(out_path))
        row_count_by_table[table_name] = len(export_rows)

    manifest = {
        "generated_at": now_utc_iso(),
        "source_snapshot_ts": selected_timestamp,
        "tables": row_count_by_table,
        "files": files_written,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "status": "success",
        "timestamp": selected_timestamp,
        "files_written": files_written + [str(manifest_path)],
        "row_count_by_table": row_count_by_table,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_prepare_raw_exports(
        args.lakehouse_gold_dir,
        args.output_dir,
        timestamp=args.timestamp,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
