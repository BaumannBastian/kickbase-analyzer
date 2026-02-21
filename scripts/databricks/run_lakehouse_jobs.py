# ------------------------------------
# run_lakehouse_jobs.py
#
# Triggert Databricks Lakehouse Jobs (bronze/silver/gold) via CLI.
# Job-IDs koennen via CLI-Argumente oder Umgebungsvariablen gesetzt
# werden.
#
# Usage
# ------------------------------------
# - python -m scripts.databricks.run_lakehouse_jobs --stage all --profile "<profile>"
# - python -m scripts.databricks.run_lakehouse_jobs --stage bronze --job-id-bronze <id>
# ------------------------------------

from __future__ import annotations

import argparse
import configparser
import os
from pathlib import Path
import subprocess
from typing import Optional


STAGE_ORDER = ["bronze", "silver", "gold"]


ENV_JOB_ID_KEYS = {
    "bronze": "DATABRICKS_JOB_ID_BRONZE",
    "silver": "DATABRICKS_JOB_ID_SILVER",
    "gold": "DATABRICKS_JOB_ID_GOLD",
}


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


def resolve_job_id(stage: str, args: argparse.Namespace) -> int:
    arg_value = {
        "bronze": args.job_id_bronze,
        "silver": args.job_id_silver,
        "gold": args.job_id_gold,
    }[stage]
    if arg_value is not None:
        return arg_value

    env_value = os.environ.get(ENV_JOB_ID_KEYS[stage], "").strip()
    if env_value:
        return int(env_value)

    raise ValueError(
        f"Missing job id for stage '{stage}'. Set --job-id-{stage} or env {ENV_JOB_ID_KEYS[stage]}."
    )


def run_command(cmd: list[str]) -> None:
    print(f">> {' '.join(cmd)}")
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def run_job(job_id: int, profile: Optional[str], no_wait: bool, timeout: Optional[str]) -> None:
    cmd = ["databricks"]
    if profile:
        cmd += ["--profile", profile]
    cmd += ["jobs", "run-now", str(job_id)]
    if no_wait:
        cmd.append("--no-wait")
    if timeout:
        cmd += ["--timeout", timeout]

    run_command(cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Databricks bronze/silver/gold jobs.")
    parser.add_argument("--stage", choices=["all", "bronze", "silver", "gold"], default="all")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--no-wait", action="store_true")
    parser.add_argument("--timeout", default=None)
    parser.add_argument("--job-id-bronze", type=int, default=None)
    parser.add_argument("--job-id-silver", type=int, default=None)
    parser.add_argument("--job-id-gold", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile = args.profile if args.profile is not None else detect_profile()

    stages = STAGE_ORDER if args.stage == "all" else [args.stage]
    print("Stages:", " -> ".join(stages))
    print("Profile:", profile or "(default)")

    for stage in stages:
        job_id = resolve_job_id(stage, args)
        print(f"Running {stage.upper()} (job_id={job_id})")
        run_job(job_id, profile=profile, no_wait=args.no_wait, timeout=args.timeout)

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
