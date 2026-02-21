# ------------------------------------
# run_bronze_ingest.py
#
# Dieses Skript laedt lokale Bronze-Rohdateien in eine
# Lakehouse-nahe Bronze-Struktur. Pro Dataset wird ein Snapshot
# unter data/lakehouse/bronze/<dataset>/ abgelegt.
#
# Outputs
# ------------------------------------
# 1) data/lakehouse/bronze/kickbase_player_snapshot/snapshot_<ts>.ndjson
# 2) data/lakehouse/bronze/kickbase_match_stats/snapshot_<ts>.ndjson
# 3) data/lakehouse/bronze/ligainsider_status_snapshot/snapshot_<ts>.ndjson
#
# Usage
# ------------------------------------
# - python -m databricks.jobs.bronze_ingest.run_bronze_ingest
# - python -m databricks.jobs.bronze_ingest.run_bronze_ingest --timestamp 2026-02-21T131500Z
# ------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from databricks.jobs.common_io import (
    find_flat_files_for_timestamp,
    latest_timestamp_common_flat,
    now_utc_iso,
    read_ndjson,
    write_ndjson,
)


DATASETS = [
    "kickbase_player_snapshot",
    "kickbase_match_stats",
    "ligainsider_status_snapshot",
]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load local bronze files into lakehouse bronze layout.")
    parser.add_argument("--bronze-dir", type=Path, default=Path("data/bronze"))
    parser.add_argument("--lakehouse-bronze-dir", type=Path, default=Path("data/lakehouse/bronze"))
    parser.add_argument("--timestamp", type=str, default=None)
    return parser.parse_args(argv)


def run_bronze_ingest(
    bronze_dir: Path,
    lakehouse_bronze_dir: Path,
    *,
    timestamp: str | None = None,
) -> dict[str, object]:
    selected_timestamp = timestamp or latest_timestamp_common_flat(bronze_dir, DATASETS)
    input_files = find_flat_files_for_timestamp(bronze_dir, DATASETS, selected_timestamp)

    loaded_at = now_utc_iso()
    files_written: list[str] = []
    rows_written = 0

    for dataset in DATASETS:
        rows = read_ndjson(input_files[dataset])
        enriched_rows: list[dict[str, object]] = []
        for row in rows:
            out = dict(row)
            out["bronze_loaded_at"] = loaded_at
            out["bronze_file_timestamp"] = selected_timestamp
            enriched_rows.append(out)

        output_path = lakehouse_bronze_dir / dataset / f"snapshot_{selected_timestamp}.ndjson"
        write_ndjson(output_path, enriched_rows)
        files_written.append(str(output_path))
        rows_written += len(enriched_rows)

    return {
        "status": "success",
        "dataset_count": len(DATASETS),
        "timestamp": selected_timestamp,
        "rows_written": rows_written,
        "files_written": files_written,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = run_bronze_ingest(
        args.bronze_dir,
        args.lakehouse_bronze_dir,
        timestamp=args.timestamp,
    )
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
