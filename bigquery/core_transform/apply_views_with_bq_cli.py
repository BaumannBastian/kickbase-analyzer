# ------------------------------------
# apply_views_with_bq_cli.py
#
# Dieses Skript erzeugt/aktualisiert CORE- und MARTS-Views in
# BigQuery per bq CLI.
#
# Outputs
# ------------------------------------
# 1) BigQuery Views in <project>.<core>
# 2) BigQuery Views in <project>.<marts>
#
# Usage
# ------------------------------------
# - python -m bigquery.core_transform.apply_views_with_bq_cli --project <project>
# ------------------------------------

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
from typing import Sequence


DEFAULT_CORE_SQL = Path("bigquery/sql/core_views.sql")
DEFAULT_MARTS_SQL = Path("bigquery/sql/marts_views.sql")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply CORE and MARTS BigQuery views via bq CLI.")
    parser.add_argument("--project", type=str, default=None)
    parser.add_argument("--raw", type=str, default="kickbase_raw")
    parser.add_argument("--core", type=str, default="kickbase_core")
    parser.add_argument("--marts", type=str, default="kickbase_marts")
    parser.add_argument("--location", type=str, default="EU")
    parser.add_argument("--core-sql", type=Path, default=DEFAULT_CORE_SQL)
    parser.add_argument("--marts-sql", type=Path, default=DEFAULT_MARTS_SQL)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def require_bq_cli() -> str:
    bq_path = shutil.which("bq")
    if bq_path is None:
        raise RuntimeError("bq CLI not found in PATH.")
    return bq_path


def run_cmd(cmd: list[str], *, dry_run: bool) -> None:
    print(">>", " ".join(cmd))
    if dry_run:
        return
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def ensure_dataset(project: str, dataset: str, location: str, *, dry_run: bool) -> None:
    cmd = [
        "bq",
        f"--location={location}",
        f"--project_id={project}",
        "mk",
        "--dataset",
        "--if_not_exists",
        f"{project}:{dataset}",
    ]
    run_cmd(cmd, dry_run=dry_run)


def render_sql(path: Path, *, project: str, raw: str, core: str, marts: str) -> str:
    template = path.read_text(encoding="utf-8")
    return template.format(project=project, raw=raw, core=core, marts=marts)


def run_query(sql: str, *, project: str, location: str, dry_run: bool) -> None:
    cmd = [
        "bq",
        f"--project_id={project}",
        f"--location={location}",
        "query",
        "--use_legacy_sql=false",
        sql,
    ]
    run_cmd(cmd, dry_run=dry_run)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project = args.project or os.environ.get("BQ_PROJECT_ID", "").strip()
    if not project:
        raise ValueError("Missing BQ project. Use --project or BQ_PROJECT_ID.")

    if args.dry_run:
        if shutil.which("bq") is None:
            print("bq CLI not found; continuing in --dry-run mode.")
    else:
        require_bq_cli()
    ensure_dataset(project, args.core, args.location, dry_run=args.dry_run)
    ensure_dataset(project, args.marts, args.location, dry_run=args.dry_run)

    core_sql = render_sql(args.core_sql, project=project, raw=args.raw, core=args.core, marts=args.marts)
    marts_sql = render_sql(args.marts_sql, project=project, raw=args.raw, core=args.core, marts=args.marts)

    run_query(core_sql, project=project, location=args.location, dry_run=args.dry_run)
    run_query(marts_sql, project=project, location=args.location, dry_run=args.dry_run)

    print("BigQuery CORE/MARTS views applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
