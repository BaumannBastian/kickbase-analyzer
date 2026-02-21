# ------------------------------------
# bronze_writer.py
#
# Dieses Modul schreibt Ingestion-Snapshots in den Bronze Layer.
# Es vereinheitlicht Dateinamen, Audit-Spalten und Run-Telemetrie
# fuer Demo- und Private-Mode.
#
# Outputs
# ------------------------------------
# 1) data/bronze/<dataset>_<timestamp>.ndjson
# 2) data/bronze/ingestion_runs.ndjson
#
# Usage
# ------------------------------------
# - Import: run_demo_ingestion(...)
# - Import: write_bronze_outputs(...)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


TIMESTAMP_FILE_FORMAT = "%Y-%m-%dT%H%M%SZ"


@dataclass(frozen=True)
class DatasetConfig:
    dataset_name: str
    source: str
    input_filename: str | None = None


DATASETS: tuple[DatasetConfig, ...] = (
    DatasetConfig(
        dataset_name="kickbase_player_snapshot",
        source="kickbase",
        input_filename="kickbase_player_snapshot.json",
    ),
    DatasetConfig(
        dataset_name="ligainsider_status_snapshot",
        source="ligainsider",
        input_filename="ligainsider_status_snapshot.json",
    ),
    DatasetConfig(
        dataset_name="odds_match_snapshot",
        source="odds_api",
        input_filename=None,
    ),
)


DATASET_CONFIG_BY_NAME: dict[str, DatasetConfig] = {
    dataset.dataset_name: dataset
    for dataset in DATASETS
}


def _resolve_datasets(
    rows_by_dataset: dict[str, list[dict[str, Any]]],
    dataset_names: list[str] | None,
) -> list[DatasetConfig]:
    requested = dataset_names or list(rows_by_dataset.keys())
    known_order = [dataset.dataset_name for dataset in DATASETS]
    ordered_names: list[str] = []

    for name in known_order:
        if name in requested and name not in ordered_names:
            ordered_names.append(name)

    for name in requested:
        if name not in ordered_names:
            ordered_names.append(name)

    configs: list[DatasetConfig] = []
    for name in ordered_names:
        config = DATASET_CONFIG_BY_NAME.get(name)
        if config is None:
            raise ValueError(f"Unknown dataset name: {name}")
        configs.append(config)
    return configs


def to_utc(dt: datetime | None = None) -> datetime:
    if dt is None:
        return datetime.now(UTC)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def format_file_timestamp(dt: datetime) -> str:
    return to_utc(dt).strftime(TIMESTAMP_FILE_FORMAT)


def format_iso_timestamp(dt: datetime) -> str:
    return to_utc(dt).isoformat().replace("+00:00", "Z")


def load_snapshot_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        raise ValueError(f"Expected list payload in {path}, got {type(payload)!r}")

    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError(f"Expected object rows in {path}, got {type(item)!r}")
        rows.append(item)
    return rows


def enrich_rows(
    rows: list[dict[str, Any]],
    *,
    ingested_at: str,
    source: str,
    source_version: str,
    run_id: str,
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        out = dict(row)
        out["ingested_at"] = ingested_at
        out["source"] = source
        out["source_version"] = source_version
        out["run_id"] = run_id
        enriched.append(out)
    return enriched


def write_ndjson(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
            f.write("\n")


def append_ndjson(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=True, sort_keys=True))
        f.write("\n")


def write_bronze_outputs(
    rows_by_dataset: dict[str, list[dict[str, Any]]],
    out_dir: Path,
    *,
    dataset_names: list[str] | None = None,
    mode: str,
    now: datetime | None = None,
    source_version: str,
) -> dict[str, Any]:
    started = to_utc(now)
    finished = started
    ingested_at = format_iso_timestamp(started)
    file_ts = format_file_timestamp(started)
    run_id = str(uuid4())

    files_written: list[str] = []
    total_rows = 0

    selected_datasets = _resolve_datasets(rows_by_dataset, dataset_names)

    for dataset in selected_datasets:
        raw_rows = rows_by_dataset.get(dataset.dataset_name, [])
        for row in raw_rows:
            if not isinstance(row, dict):
                raise ValueError(
                    f"Dataset {dataset.dataset_name} contains non-object rows: {type(row)!r}"
                )
        enriched = enrich_rows(
            raw_rows,
            ingested_at=ingested_at,
            source=dataset.source,
            source_version=source_version,
            run_id=run_id,
        )
        output_name = f"{dataset.dataset_name}_{file_ts}.ndjson"
        output_path = out_dir / output_name
        write_ndjson(output_path, enriched)
        files_written.append(output_path.name)
        total_rows += len(enriched)

    run_row = {
        "run_id": run_id,
        "mode": mode,
        "status": "success",
        "started_at": ingested_at,
        "finished_at": format_iso_timestamp(finished),
        "rows_written": total_rows,
        "files_written": files_written,
    }
    append_ndjson(out_dir / "ingestion_runs.ndjson", run_row)

    summary = dict(run_row)
    summary["out_dir"] = str(out_dir)
    return summary


def run_demo_ingestion(
    demo_dir: Path,
    out_dir: Path,
    *,
    now: datetime | None = None,
    source_version: str = "demo-v1",
) -> dict[str, Any]:
    rows_by_dataset: dict[str, list[dict[str, Any]]] = {}

    for dataset in DATASETS:
        if dataset.input_filename is None:
            continue
        input_path = demo_dir / dataset.input_filename
        rows_by_dataset[dataset.dataset_name] = load_snapshot_rows(input_path)

    return write_bronze_outputs(
        rows_by_dataset,
        out_dir,
        dataset_names=[dataset.dataset_name for dataset in DATASETS if dataset.input_filename is not None],
        mode="demo",
        now=now,
        source_version=source_version,
    )
