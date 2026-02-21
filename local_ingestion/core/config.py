# ------------------------------------
# config.py
#
# Dieses Modul laedt Laufzeitkonfiguration fuer den private
# Ingestion-Mode aus Umgebungsvariablen und optional aus `.env`.
#
# Outputs
# ------------------------------------
# 1) Kein Dateiausgabe-Artefakt; liefert typisierte Config-Objekte.
#
# Usage
# ------------------------------------
# - config = load_private_ingestion_config(Path(".env"))
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class RetryConfig:
    timeout_seconds: float
    max_retries: int
    backoff_seconds: float
    rate_limit_seconds: float


@dataclass(frozen=True)
class PrivateIngestionConfig:
    base_url: str
    league_id: str
    email: str
    password: str
    source_version: str
    auth_path: str
    player_snapshot_path: str
    match_stats_path: str
    cache_dir: Path
    cache_ttl_seconds: int
    ligainsider_status_file: Path | None
    retry: RetryConfig


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _strip_quotes(value.strip())
        os.environ.setdefault(key, value)


def _get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_int_env(name: str, *, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {name}: {raw!r}") from exc


def _get_float_env(name: str, *, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}: {raw!r}") from exc


def load_private_ingestion_config(env_file: Path | None = None) -> PrivateIngestionConfig:
    if env_file is not None:
        load_dotenv_file(env_file)

    base_url = _get_required_env("KICKBASE_BASE_URL").rstrip("/")
    league_id = _get_required_env("KICKBASE_LEAGUE_ID")
    email = _get_required_env("KICKBASE_EMAIL")
    password = _get_required_env("KICKBASE_PASSWORD")

    source_version = os.environ.get("SOURCE_VERSION", "private-v1").strip() or "private-v1"
    auth_path = os.environ.get("KICKBASE_AUTH_PATH", "/auth/login").strip() or "/auth/login"
    player_snapshot_path = (
        os.environ.get("KICKBASE_PLAYER_SNAPSHOT_PATH", "/leagues/{league_id}/players/snapshot")
        .strip()
        or "/leagues/{league_id}/players/snapshot"
    )
    match_stats_path = (
        os.environ.get("KICKBASE_MATCH_STATS_PATH", "/leagues/{league_id}/players/match-stats")
        .strip()
        or "/leagues/{league_id}/players/match-stats"
    )

    retry = RetryConfig(
        timeout_seconds=_get_float_env("KICKBASE_TIMEOUT_SECONDS", default=20.0),
        max_retries=_get_int_env("KICKBASE_MAX_RETRIES", default=2),
        backoff_seconds=_get_float_env("KICKBASE_BACKOFF_SECONDS", default=1.0),
        rate_limit_seconds=_get_float_env("KICKBASE_RATE_LIMIT_SECONDS", default=0.25),
    )

    if retry.max_retries < 0:
        raise ValueError("KICKBASE_MAX_RETRIES must be >= 0")
    if retry.timeout_seconds <= 0:
        raise ValueError("KICKBASE_TIMEOUT_SECONDS must be > 0")
    if retry.backoff_seconds < 0:
        raise ValueError("KICKBASE_BACKOFF_SECONDS must be >= 0")
    if retry.rate_limit_seconds < 0:
        raise ValueError("KICKBASE_RATE_LIMIT_SECONDS must be >= 0")

    cache_dir = Path(os.environ.get("KICKBASE_CACHE_DIR", "data/cache/kickbase"))
    cache_ttl_seconds = _get_int_env("KICKBASE_CACHE_TTL_SECONDS", default=300)

    ligainsider_raw = os.environ.get("LIGAINSIDER_STATUS_FILE", "").strip()
    ligainsider_status_file = Path(ligainsider_raw) if ligainsider_raw else None

    return PrivateIngestionConfig(
        base_url=base_url,
        league_id=league_id,
        email=email,
        password=password,
        source_version=source_version,
        auth_path=auth_path,
        player_snapshot_path=player_snapshot_path,
        match_stats_path=match_stats_path,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        ligainsider_status_file=ligainsider_status_file,
        retry=retry,
    )
