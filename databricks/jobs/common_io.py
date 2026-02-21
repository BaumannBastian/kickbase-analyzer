# ------------------------------------
# common_io.py
#
# Gemeinsame I/O-Helfer fuer lokale Bronze/Silver/Gold Jobs.
# Das Modul liest/schreibt NDJSON und CSV und ermittelt jeweils
# die neuesten Snapshot-Dateien pro Dataset.
#
# Outputs
# ------------------------------------
# 1) Beliebige NDJSON-Dateien ueber write_ndjson(...)
# 2) Beliebige CSV-Dateien ueber write_csv(...)
#
# Usage
# ------------------------------------
# - read_ndjson(path)
# - write_ndjson(path, rows)
# - find_latest_partitioned_files(root, dataset_names)
# ------------------------------------

from __future__ import annotations

import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any


SNAPSHOT_TIMESTAMP_RE = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{6}Z)")


def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def extract_timestamp_from_path(path: Path) -> str:
    match = SNAPSHOT_TIMESTAMP_RE.search(path.name)
    if match is None:
        raise ValueError(f"Could not extract timestamp from filename: {path}")
    return match.group("ts")


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError(f"Expected JSON object rows in {path}, got {type(payload)!r}")
        rows.append(payload)
    return rows


def write_ndjson(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            f.write("\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def latest_timestamp_common_flat(bronze_dir: Path, dataset_names: list[str]) -> str:
    timestamp_sets: list[set[str]] = []

    for dataset in dataset_names:
        timestamps: set[str] = set()
        for path in bronze_dir.glob(f"{dataset}_*.ndjson"):
            timestamps.add(extract_timestamp_from_path(path))
        if not timestamps:
            raise FileNotFoundError(f"No bronze files found for dataset: {dataset}")
        timestamp_sets.append(timestamps)

    common = set.intersection(*timestamp_sets)
    if not common:
        raise FileNotFoundError("No common timestamp across all bronze datasets")
    return sorted(common)[-1]


def latest_timestamp_common_partitioned(root_dir: Path, dataset_names: list[str]) -> str:
    timestamp_sets: list[set[str]] = []

    for dataset in dataset_names:
        dataset_dir = root_dir / dataset
        if not dataset_dir.exists():
            raise FileNotFoundError(f"Missing dataset directory: {dataset_dir}")
        timestamps: set[str] = set()
        for path in dataset_dir.glob("*.ndjson"):
            timestamps.add(extract_timestamp_from_path(path))
        if not timestamps:
            raise FileNotFoundError(f"No snapshot files found for dataset: {dataset}")
        timestamp_sets.append(timestamps)

    common = set.intersection(*timestamp_sets)
    if not common:
        raise FileNotFoundError("No common timestamp across required datasets")
    return sorted(common)[-1]


def find_flat_files_for_timestamp(
    bronze_dir: Path,
    dataset_names: list[str],
    timestamp: str,
) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for dataset in dataset_names:
        path = bronze_dir / f"{dataset}_{timestamp}.ndjson"
        if not path.exists():
            raise FileNotFoundError(f"Missing bronze file: {path}")
        files[dataset] = path
    return files


def find_partitioned_files_for_timestamp(
    root_dir: Path,
    dataset_names: list[str],
    timestamp: str,
) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for dataset in dataset_names:
        path = root_dir / dataset / f"snapshot_{timestamp}.ndjson"
        if not path.exists():
            raise FileNotFoundError(f"Missing snapshot file: {path}")
        files[dataset] = path
    return files
