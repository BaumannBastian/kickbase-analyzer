# ------------------------------------
# sync_repo.py
#
# Sync (pull) ein Databricks Repo auf den neuesten Git-Stand.
# Optional kann bei Konflikten ein Reset ausgefuehrt werden:
# backup -> delete -> recreate -> branch checkout.
#
# Usage
# ------------------------------------
# - python -m scripts.databricks.sync_repo --repo-path "/Repos/<user>/kickbase-analyzer"
# - python -m scripts.databricks.sync_repo --repo-path "/Repos/<user>/kickbase-analyzer" --reset-if-conflict --backup
# ------------------------------------

from __future__ import annotations

import argparse
import configparser
import json
import os
from pathlib import Path
import subprocess
import time
from datetime import datetime
from typing import Optional, Tuple


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


def run_cmd(cmd: list[str], *, capture: bool = False) -> Tuple[int, str, str]:
    proc = subprocess.run(cmd, capture_output=capture, text=True)
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    return proc.returncode, stdout, stderr


def run_or_raise(cmd: list[str], *, capture: bool = False) -> str:
    print(f">> {' '.join(cmd)}")
    rc, out, err = run_cmd(cmd, capture=capture)
    if rc != 0:
        raise RuntimeError(err or out or f"Command failed with rc={rc}")
    return out if capture else ""


def normalize_git_url(url: str) -> str:
    if url.startswith("https://github.com/") and not url.endswith(".git"):
        return f"{url.rstrip('/')}.git"
    return url


def dbx(profile: Optional[str], *args: str) -> list[str]:
    cmd = ["databricks"]
    if profile:
        cmd += ["--profile", profile]
    cmd += list(args)
    return cmd


def repos_get(ref: str, profile: Optional[str]) -> Optional[dict[str, object]]:
    rc, out, _ = run_cmd(dbx(profile, "--output", "json", "repos", "get", ref), capture=True)
    if rc != 0 or not out:
        return None
    loaded = json.loads(out)
    if not isinstance(loaded, dict):
        return None
    return loaded


def repos_update(ref: str, branch: str, profile: Optional[str]) -> None:
    run_or_raise(dbx(profile, "repos", "update", ref, "--branch", branch))


def repos_delete(ref: str, profile: Optional[str]) -> None:
    run_or_raise(dbx(profile, "repos", "delete", ref))


def repos_create(git_url: str, provider: str, repo_path: str, profile: Optional[str]) -> None:
    run_or_raise(dbx(profile, "repos", "create", git_url, provider, "--path", repo_path))


def backup_workspace_dir(source_path: str, target_dir: Path, profile: Optional[str]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    run_or_raise(
        dbx(profile, "workspace", "export-dir", source_path, str(target_dir), "--overwrite")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Databricks Repo to latest Git branch state.")
    parser.add_argument("--profile", default=None)
    parser.add_argument("--repo-path", default=None)
    parser.add_argument("--repo-id", default=None)
    parser.add_argument("--branch", default="main")
    parser.add_argument(
        "--git-url",
        default="https://github.com/BaumannBastian/kickbase-analyzer",
    )
    parser.add_argument("--provider", default="gitHub")
    parser.add_argument("--reset-if-conflict", action="store_true")
    parser.add_argument("--backup", action="store_true")
    parser.add_argument("--backup-dir", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.repo_path and not args.repo_id:
        raise SystemExit("Please provide --repo-path or --repo-id.")

    profile = args.profile if args.profile is not None else detect_profile()
    ref = str(args.repo_id) if args.repo_id else str(args.repo_path)
    git_url = normalize_git_url(str(args.git_url))

    print(f"Ref: {ref}")
    print(f"Branch: {args.branch}")
    print(f"Profile: {profile or '(default)'}")

    info = repos_get(ref, profile)
    if info is None:
        if args.repo_path is None:
            raise SystemExit("Repo not found via --repo-id. Provide --repo-path to allow creation.")
        print("Repo not found. Creating it now.")
        repos_create(git_url, args.provider, str(args.repo_path), profile)
        info = repos_get(str(args.repo_path), profile)

    repo_path = str((info or {}).get("path") or args.repo_path or "")

    try:
        print("Syncing repo (repos update)...")
        repos_update(ref, str(args.branch), profile)
        print("Repo synced.")
        return 0
    except Exception as exc:
        print("repos update failed.")
        print(str(exc))
        if not args.reset_if_conflict:
            raise SystemExit("Run with --reset-if-conflict to force reset.")

    if args.backup:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = (
            Path(args.backup_dir)
            if args.backup_dir
            else Path(".tmp/databricks_repo_backup") / timestamp
        )
        print(f"Backing up workspace path to {backup_dir}")
        backup_workspace_dir(repo_path, backup_dir, profile)

    print("Hard reset: delete + recreate + branch sync")
    repos_delete(ref, profile)
    time.sleep(1)

    if not repo_path:
        raise SystemExit("Cannot recreate repo because repo path is unknown. Provide --repo-path.")

    repos_create(git_url, args.provider, repo_path, profile)
    repos_update(repo_path, str(args.branch), profile)
    print("Repo hard-reset + synced.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
