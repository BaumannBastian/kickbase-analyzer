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
    kickbase_user_agent: str
    source_version: str
    auth_path: str
    auth_email_field: str
    auth_password_field: str
    player_snapshot_path: str
    competition_id: str | None
    competition_players_search_path: str
    competition_players_page_size: int
    competition_players_query: str
    match_stats_path: str
    player_details_path: str
    player_market_value_history_path: str
    player_performance_path: str
    player_transfers_path: str
    cache_dir: Path
    cache_ttl_seconds: int
    odds_api_key: str | None
    odds_base_url: str
    odds_sport_key: str
    odds_regions: str
    odds_markets: str
    odds_odds_format: str
    odds_date_format: str
    odds_bookmakers: str | None
    odds_match_limit: int
    odds_retry: RetryConfig
    ligainsider_status_url: str | None
    ligainsider_user_agent: str
    ligainsider_retry: RetryConfig
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
    kickbase_user_agent = (
        os.environ.get("KICKBASE_USER_AGENT", "okhttp/4.11.0").strip() or "okhttp/4.11.0"
    )

    source_version = os.environ.get("SOURCE_VERSION", "private-v1").strip() or "private-v1"
    auth_path = os.environ.get("KICKBASE_AUTH_PATH", "/v4/user/login").strip() or "/v4/user/login"
    auth_email_field = (
        os.environ.get("KICKBASE_AUTH_EMAIL_FIELD", "em").strip() or "em"
    )
    auth_password_field = (
        os.environ.get("KICKBASE_AUTH_PASSWORD_FIELD", "pass").strip() or "pass"
    )
    player_snapshot_path = (
        os.environ.get("KICKBASE_PLAYER_SNAPSHOT_PATH", "/v4/leagues/{league_id}/market")
        .strip()
        or "/v4/leagues/{league_id}/market"
    )
    competition_id = os.environ.get("KICKBASE_COMPETITION_ID", "").strip() or None
    competition_players_search_path = (
        os.environ.get(
            "KICKBASE_COMPETITION_PLAYERS_SEARCH_PATH",
            "/v4/competitions/{competition_id}/players/search",
        )
        .strip()
        or "/v4/competitions/{competition_id}/players/search"
    )
    competition_players_page_size = _get_int_env("KICKBASE_COMPETITION_PLAYERS_PAGE_SIZE", default=100)
    if competition_players_page_size <= 0:
        raise ValueError("KICKBASE_COMPETITION_PLAYERS_PAGE_SIZE must be > 0")
    competition_players_query = os.environ.get("KICKBASE_COMPETITION_PLAYERS_QUERY", "")
    match_stats_path = (
        os.environ.get("KICKBASE_MATCH_STATS_PATH", "/v4/leagues/{league_id}/lineup")
        .strip()
        or "/v4/leagues/{league_id}/lineup"
    )
    player_details_path = (
        os.environ.get(
            "KICKBASE_PLAYER_DETAILS_PATH",
            "/v4/leagues/{league_id}/players/{player_id}",
        )
        .strip()
        or "/v4/leagues/{league_id}/players/{player_id}"
    )
    player_market_value_history_path = (
        os.environ.get(
            "KICKBASE_PLAYER_MARKET_VALUE_HISTORY_PATH",
            "/v4/players/{player_id}/market-value",
        )
        .strip()
        or "/v4/players/{player_id}/market-value"
    )
    player_performance_path = (
        os.environ.get("KICKBASE_PLAYER_PERFORMANCE_PATH", "/v4/players/{player_id}/performance")
        .strip()
        or "/v4/players/{player_id}/performance"
    )
    player_transfers_path = (
        os.environ.get(
            "KICKBASE_PLAYER_TRANSFERS_PATH",
            "/v4/leagues/{league_id}/players/{player_id}/transfers",
        )
        .strip()
        or "/v4/leagues/{league_id}/players/{player_id}/transfers"
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

    odds_api_key = os.environ.get("ODDS_API_KEY", "").strip() or None
    odds_base_url = (
        os.environ.get("ODDS_BASE_URL", "https://api.the-odds-api.com/v4").strip()
        or "https://api.the-odds-api.com/v4"
    )
    odds_sport_key = (
        os.environ.get("ODDS_SPORT_KEY", "soccer_germany_bundesliga").strip()
        or "soccer_germany_bundesliga"
    )
    odds_regions = os.environ.get("ODDS_REGIONS", "eu").strip() or "eu"
    odds_markets = os.environ.get("ODDS_MARKETS", "h2h,totals").strip() or "h2h,totals"
    odds_odds_format = os.environ.get("ODDS_ODDS_FORMAT", "decimal").strip() or "decimal"
    odds_date_format = os.environ.get("ODDS_DATE_FORMAT", "iso").strip() or "iso"
    odds_bookmakers = os.environ.get("ODDS_BOOKMAKERS", "").strip() or None
    odds_match_limit = _get_int_env("ODDS_MATCH_LIMIT", default=9)
    if odds_match_limit <= 0:
        raise ValueError("ODDS_MATCH_LIMIT must be > 0")

    odds_retry = RetryConfig(
        timeout_seconds=_get_float_env("ODDS_TIMEOUT_SECONDS", default=retry.timeout_seconds),
        max_retries=_get_int_env("ODDS_MAX_RETRIES", default=retry.max_retries),
        backoff_seconds=_get_float_env("ODDS_BACKOFF_SECONDS", default=retry.backoff_seconds),
        rate_limit_seconds=_get_float_env(
            "ODDS_RATE_LIMIT_SECONDS",
            default=max(0.25, retry.rate_limit_seconds),
        ),
    )
    if odds_retry.max_retries < 0:
        raise ValueError("ODDS_MAX_RETRIES must be >= 0")
    if odds_retry.timeout_seconds <= 0:
        raise ValueError("ODDS_TIMEOUT_SECONDS must be > 0")
    if odds_retry.backoff_seconds < 0:
        raise ValueError("ODDS_BACKOFF_SECONDS must be >= 0")
    if odds_retry.rate_limit_seconds < 0:
        raise ValueError("ODDS_RATE_LIMIT_SECONDS must be >= 0")

    ligainsider_status_url = os.environ.get("LIGAINSIDER_STATUS_URL", "").strip() or None
    ligainsider_user_agent = (
        os.environ.get("LIGAINSIDER_USER_AGENT", "kickbase-analyzer/0.1 (+private-use)")
        .strip()
        or "kickbase-analyzer/0.1 (+private-use)"
    )
    ligainsider_retry = RetryConfig(
        timeout_seconds=_get_float_env(
            "LIGAINSIDER_TIMEOUT_SECONDS",
            default=retry.timeout_seconds,
        ),
        max_retries=_get_int_env(
            "LIGAINSIDER_MAX_RETRIES",
            default=retry.max_retries,
        ),
        backoff_seconds=_get_float_env(
            "LIGAINSIDER_BACKOFF_SECONDS",
            default=retry.backoff_seconds,
        ),
        rate_limit_seconds=_get_float_env(
            "LIGAINSIDER_RATE_LIMIT_SECONDS",
            default=max(1.0, retry.rate_limit_seconds),
        ),
    )

    if ligainsider_retry.max_retries < 0:
        raise ValueError("LIGAINSIDER_MAX_RETRIES must be >= 0")
    if ligainsider_retry.timeout_seconds <= 0:
        raise ValueError("LIGAINSIDER_TIMEOUT_SECONDS must be > 0")
    if ligainsider_retry.backoff_seconds < 0:
        raise ValueError("LIGAINSIDER_BACKOFF_SECONDS must be >= 0")
    if ligainsider_retry.rate_limit_seconds < 0:
        raise ValueError("LIGAINSIDER_RATE_LIMIT_SECONDS must be >= 0")

    ligainsider_raw = os.environ.get("LIGAINSIDER_STATUS_FILE", "").strip()
    ligainsider_status_file = Path(ligainsider_raw) if ligainsider_raw else None

    return PrivateIngestionConfig(
        base_url=base_url,
        league_id=league_id,
        email=email,
        password=password,
        kickbase_user_agent=kickbase_user_agent,
        source_version=source_version,
        auth_path=auth_path,
        auth_email_field=auth_email_field,
        auth_password_field=auth_password_field,
        player_snapshot_path=player_snapshot_path,
        competition_id=competition_id,
        competition_players_search_path=competition_players_search_path,
        competition_players_page_size=competition_players_page_size,
        competition_players_query=competition_players_query,
        match_stats_path=match_stats_path,
        player_details_path=player_details_path,
        player_market_value_history_path=player_market_value_history_path,
        player_performance_path=player_performance_path,
        player_transfers_path=player_transfers_path,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        odds_api_key=odds_api_key,
        odds_base_url=odds_base_url,
        odds_sport_key=odds_sport_key,
        odds_regions=odds_regions,
        odds_markets=odds_markets,
        odds_odds_format=odds_odds_format,
        odds_date_format=odds_date_format,
        odds_bookmakers=odds_bookmakers,
        odds_match_limit=odds_match_limit,
        odds_retry=odds_retry,
        ligainsider_status_url=ligainsider_status_url,
        ligainsider_user_agent=ligainsider_user_agent,
        ligainsider_retry=ligainsider_retry,
        ligainsider_status_file=ligainsider_status_file,
        retry=retry,
    )
