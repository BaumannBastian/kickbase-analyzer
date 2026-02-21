# ------------------------------------
# update_jobs_to_repo.py
#
# Aktualisiert Databricks Jobs so, dass Notebook-Tasks auf
# Repo-Notebooks unter /Repos/... zeigen.
#
# Usage
# ------------------------------------
# - python -m scripts.databricks.update_jobs_to_repo --repo-path "/Repos/<user>/kickbase-analyzer" --profile "<profile>"
# ------------------------------------

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from typing import Optional


DEFAULT_TASKKEY_TO_NOTEBOOK = {
    "kickbase_bronze": "databricks/notebooks/lakehouse/01_bronze_job",
    "kickbase_silver": "databricks/notebooks/lakehouse/02_silver_job",
    "kickbase_gold": "databricks/notebooks/lakehouse/03_gold_job",
}


def dbx(profile: Optional[str], *args: str) -> list[str]:
    cmd = ["databricks"]
    if profile:
        cmd += ["--profile", profile]
    cmd += list(args)
    return cmd


def run_json(cmd: list[str]) -> dict[str, object]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"Command failed: {' '.join(cmd)}")
    raw = (proc.stdout or "").strip()
    if not raw:
        raise RuntimeError(f"Empty JSON output: {' '.join(cmd)}")
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Unexpected JSON payload type: {type(loaded)!r}")
    return loaded


def run_cmd(cmd: list[str]) -> None:
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def update_job(job_id: int, repo_path: str, profile: Optional[str]) -> None:
    job = run_json(dbx(profile, "jobs", "get", str(job_id), "--output", "json"))
    settings = job.get("settings")
    if not isinstance(settings, dict):
        raise RuntimeError(f"Unexpected job format for job_id={job_id}: missing settings")

    tasks = settings.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RuntimeError(f"Unexpected job format for job_id={job_id}: missing tasks[]")

    changed = False
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_key = str(task.get("task_key", ""))
        notebook_task = task.get("notebook_task")
        if not isinstance(notebook_task, dict):
            continue

        relative = DEFAULT_TASKKEY_TO_NOTEBOOK.get(task_key)
        if relative is None:
            continue

        target = f"{repo_path.rstrip('/')}/{relative}"
        if notebook_task.get("notebook_path") != target:
            notebook_task["notebook_path"] = target
            changed = True

    if not changed:
        print(f"job_id={job_id}: nothing to update")
        return

    payload = {
        "job_id": job_id,
        "new_settings": settings,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(payload, tmp, indent=2)
        tmp_path = tmp.name

    print(f"Updating job_id={job_id} -> notebooks from {repo_path}")
    run_cmd(dbx(profile, "jobs", "reset", "--json", f"@{tmp_path}"))
    print("OK")


def parse_job_ids(raw: str) -> list[int]:
    values: list[int] = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        values.append(int(token))
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Point Databricks jobs to repo notebook paths.")
    parser.add_argument("--repo-path", required=True)
    parser.add_argument("--profile", default=None)
    parser.add_argument(
        "--job-ids",
        default=os.environ.get("DATABRICKS_JOB_IDS", ""),
        help="Comma-separated list of job ids.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    job_ids = parse_job_ids(str(args.job_ids))
    if not job_ids:
        raise SystemExit("No job ids provided. Use --job-ids or env DATABRICKS_JOB_IDS.")

    for job_id in job_ids:
        update_job(job_id=job_id, repo_path=str(args.repo_path), profile=args.profile)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
