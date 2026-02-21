# ------------------------------------
# load_raw_with_bq_cli.py
#
# Dieses Skript laedt RAW JSONL Exporte per bq CLI nach BigQuery.
#
# Outputs
# ------------------------------------
# 1) BigQuery Tabellen in <project>.<dataset>
#
# Usage
# ------------------------------------
# - python -m bigquery.raw_load.load_raw_with_bq_cli --project <project> --dataset kickbase_raw
# - python -m bigquery.raw_load.load_raw_with_bq_cli --project <project> --dataset kickbase_raw --write-disposition truncate
# ------------------------------------

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
from typing import Sequence


TABLE_FILE_MAP = {
    "feat_player_daily": "feat_player_daily.jsonl",
    "points_components_matchday": "points_components_matchday.jsonl",
    "quality_metrics": "quality_metrics.jsonl",
}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load RAW JSONL exports into BigQuery via bq CLI.")
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--dataset", type=str, default="kickbase_raw")
    parser.add_argument("--location", type=str, default="EU")
    parser.add_argument("--input-dir", type=Path, default=Path("data/warehouse/raw"))
    parser.add_argument("--write-disposition", choices=["append", "truncate"], default="append")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def resolve_bq_cli() -> str:
    override = os.environ.get("BQ_CLI_PATH", "").strip()
    if override:
        return override

    for candidate in ("bq", "bq.cmd"):
        bq_path = shutil.which(candidate)
        if bq_path is not None:
            return bq_path

    local_appdata = os.environ.get("LOCALAPPDATA", "").strip()
    if local_appdata:
        fallback = Path(local_appdata) / "Google" / "Cloud SDK" / "google-cloud-sdk" / "bin" / "bq.cmd"
        if fallback.exists():
            return str(fallback)

    raise RuntimeError("bq CLI not found in PATH. Set BQ_CLI_PATH or add bq/bq.cmd to PATH.")


def run_cmd(cmd: list[str], *, dry_run: bool) -> None:
    print(">>", " ".join(cmd))
    if dry_run:
        return
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def dataset_exists(project: str, dataset: str, *, bq_cli: str, dry_run: bool) -> bool:
    cmd = [
        bq_cli,
        f"--project_id={project}",
        "show",
        "--format=none",
        f"{project}:{dataset}",
    ]
    print(">>", " ".join(cmd))
    if dry_run:
        return False
    proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc.returncode == 0


def ensure_dataset(
    project: str,
    dataset: str,
    location: str,
    *,
    bq_cli: str,
    dry_run: bool,
) -> None:
    if dataset_exists(project, dataset, bq_cli=bq_cli, dry_run=dry_run):
        print(f"Dataset already exists: {project}:{dataset}")
        return

    cmd = [
        bq_cli,
        f"--location={location}",
        f"--project_id={project}",
        "mk",
        "--dataset",
        f"{project}:{dataset}",
    ]
    run_cmd(cmd, dry_run=dry_run)


def load_table(
    *,
    project: str,
    dataset: str,
    table: str,
    path: Path,
    write_disposition: str,
    bq_cli: str,
    dry_run: bool,
) -> None:
    table_ref = f"{project}:{dataset}.{table}"
    cmd = [
        bq_cli,
        f"--project_id={project}",
        "load",
        "--autodetect",
        "--source_format=NEWLINE_DELIMITED_JSON",
    ]

    if write_disposition == "truncate":
        cmd.append("--replace")

    cmd += [table_ref, str(path)]
    run_cmd(cmd, dry_run=dry_run)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project = args.project or os.environ.get("BQ_PROJECT_ID", "").strip()
    if not project:
        raise ValueError("Missing BQ project. Use --project or BQ_PROJECT_ID.")

    if args.dry_run:
        try:
            bq_cli = resolve_bq_cli()
        except RuntimeError:
            bq_cli = "bq"
            print("bq CLI not found; continuing in --dry-run mode.")
    else:
        bq_cli = resolve_bq_cli()
    ensure_dataset(
        project,
        args.dataset,
        args.location,
        bq_cli=bq_cli,
        dry_run=args.dry_run,
    )

    for table, filename in TABLE_FILE_MAP.items():
        file_path = args.input_dir / filename
        if not file_path.exists():
            print(f"Skip {table}: missing file {file_path}")
            continue
        load_table(
            project=project,
            dataset=args.dataset,
            table=table,
            path=file_path,
            write_disposition=args.write_disposition,
            bq_cli=bq_cli,
            dry_run=args.dry_run,
        )

    print("BigQuery RAW load finished.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
