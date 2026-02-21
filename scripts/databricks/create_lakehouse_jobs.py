# ------------------------------------
# create_lakehouse_jobs.py
#
# Legt die drei Databricks Lakehouse Jobs fuer kickbase_bronze,
# kickbase_silver und kickbase_gold an oder aktualisiert sie.
# Die Jobs werden auf Notebook-Pfade innerhalb des Databricks Repos
# unter /Repos/... verdrahtet.
#
# Usage
# ------------------------------------
# - python -m scripts.databricks.create_lakehouse_jobs --repo-path "/Repos/<user>/kickbase-analyzer"
# - python -m scripts.databricks.create_lakehouse_jobs --repo-path "/Repos/<user>/kickbase-analyzer" --profile "<profile>"
# - python -m scripts.databricks.create_lakehouse_jobs --repo-path "/Repos/<user>/kickbase-analyzer" --dry-run
# ------------------------------------

from __future__ import annotations

import argparse
import configparser
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Optional


TASK_SPECS = [
    {
        "stage": "bronze",
        "job_name": "kickbase_bronze",
        "task_key": "kickbase_bronze",
        "notebook_relative": "databricks/notebooks/lakehouse/01_bronze_job",
    },
    {
        "stage": "silver",
        "job_name": "kickbase_silver",
        "task_key": "kickbase_silver",
        "notebook_relative": "databricks/notebooks/lakehouse/02_silver_job",
    },
    {
        "stage": "gold",
        "job_name": "kickbase_gold",
        "task_key": "kickbase_gold",
        "notebook_relative": "databricks/notebooks/lakehouse/03_gold_job",
    },
]


def detect_profile() -> Optional[str]:
    for env_key in ("DATABRICKS_PROFILE", "DATABRICKS_CONFIG_PROFILE"):
        value = os.environ.get(env_key)
        if value:
            return value

    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return None

    cp = configparser.ConfigParser()
    cp.read(cfg_path, encoding="utf-8")

    if cp.has_section("DEFAULT"):
        return None

    sections = [section for section in cp.sections() if section.strip()]
    if len(sections) == 1:
        return sections[0]
    return None


def dbx(profile: Optional[str], *args: str) -> list[str]:
    cmd = ["databricks"]
    if profile:
        cmd += ["--profile", profile]
    cmd += list(args)
    return cmd


def run_json(cmd: list[str]) -> object:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"Command failed: {' '.join(cmd)}")
    raw = (proc.stdout or "").strip()
    if not raw:
        raise RuntimeError(f"Empty output from command: {' '.join(cmd)}")
    return json.loads(raw)


def run_cmd(cmd: list[str]) -> None:
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed with rc={proc.returncode}: {' '.join(cmd)}")


def list_jobs(profile: Optional[str]) -> list[dict[str, object]]:
    payload = run_json(dbx(profile, "jobs", "list", "--output", "json"))
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected jobs list payload type: {type(payload)!r}")

    jobs: list[dict[str, object]] = []
    for item in payload:
        if isinstance(item, dict):
            jobs.append(item)
    return jobs


def find_job_id_by_name(job_name: str, jobs: list[dict[str, object]]) -> Optional[int]:
    for job in jobs:
        settings = job.get("settings")
        if not isinstance(settings, dict):
            continue
        if str(settings.get("name", "")) != job_name:
            continue

        raw_job_id = job.get("job_id")
        if isinstance(raw_job_id, int):
            return raw_job_id
        if isinstance(raw_job_id, str) and raw_job_id.isdigit():
            return int(raw_job_id)
    return None


def build_job_settings(repo_path: str, task_key: str, job_name: str, notebook_relative: str) -> dict[str, object]:
    notebook_path = f"{repo_path.rstrip('/')}/{notebook_relative}"
    return {
        "name": job_name,
        "format": "MULTI_TASK",
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "timeout_seconds": 0,
        "environments": [
            {
                "environment_key": "Default",
                "spec": {"environment_version": "4"},
            }
        ],
        "tasks": [
            {
                "task_key": task_key,
                "run_if": "ALL_SUCCESS",
                "timeout_seconds": 0,
                "environment_key": "Default",
                "email_notifications": {},
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ],
    }


def write_json_temp(payload: dict[str, object]) -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(payload, tmp, indent=2)
        return tmp.name


def create_job(profile: Optional[str], payload: dict[str, object]) -> int:
    tmp_path = write_json_temp(payload)
    result = run_json(dbx(profile, "jobs", "create", "--json", f"@{tmp_path}"))
    if not isinstance(result, dict):
        raise RuntimeError(f"Unexpected jobs create payload: {type(result)!r}")
    raw_job_id = result.get("job_id")
    if isinstance(raw_job_id, int):
        return raw_job_id
    if isinstance(raw_job_id, str) and raw_job_id.isdigit():
        return int(raw_job_id)
    raise RuntimeError("jobs create response did not contain job_id.")


def reset_job(profile: Optional[str], job_id: int, payload: dict[str, object]) -> None:
    reset_payload = {
        "job_id": job_id,
        "new_settings": payload,
    }
    tmp_path = write_json_temp(reset_payload)
    run_cmd(dbx(profile, "jobs", "reset", "--json", f"@{tmp_path}"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update kickbase lakehouse jobs in Databricks.")
    parser.add_argument("--repo-path", required=True, help='Databricks repo path, e.g. "/Repos/<user>/kickbase-analyzer"')
    parser.add_argument("--profile", default=None, help="Databricks CLI profile (optional)")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile = args.profile if args.profile is not None else detect_profile()
    jobs = list_jobs(profile)

    print("Profile:", profile or "(default)")
    print("Repo path:", args.repo_path)

    resolved: dict[str, int] = {}
    for spec in TASK_SPECS:
        job_name = str(spec["job_name"])
        task_key = str(spec["task_key"])
        notebook_relative = str(spec["notebook_relative"])
        stage = str(spec["stage"])

        settings_payload = build_job_settings(
            repo_path=str(args.repo_path),
            task_key=task_key,
            job_name=job_name,
            notebook_relative=notebook_relative,
        )

        existing_job_id = find_job_id_by_name(job_name, jobs)
        if existing_job_id is None:
            if args.dry_run:
                print(f"[DRY-RUN] CREATE {job_name}")
                continue

            new_job_id = create_job(profile, settings_payload)
            resolved[stage] = new_job_id
            print(f"[CREATE] {job_name} -> job_id={new_job_id}")
            jobs = list_jobs(profile)
            continue

        if args.dry_run:
            print(f"[DRY-RUN] RESET {job_name} (job_id={existing_job_id})")
            resolved[stage] = existing_job_id
            continue

        reset_job(profile, existing_job_id, settings_payload)
        resolved[stage] = existing_job_id
        print(f"[RESET] {job_name} -> job_id={existing_job_id}")

    if resolved:
        print("")
        print("Export values:")
        print(f"export DATABRICKS_JOB_ID_BRONZE={resolved.get('bronze', '')}")
        print(f"export DATABRICKS_JOB_ID_SILVER={resolved.get('silver', '')}")
        print(f"export DATABRICKS_JOB_ID_GOLD={resolved.get('gold', '')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
