# ------------------------------------
# kickbase_client.py
#
# HTTP-Client fuer Kickbase-Endpunkte, die fuer History-ETL benoetigt
# werden (Login, Matchdays, Eventtypen, Marktwerte, Performance,
# Playercenter).
#
# Usage
# ------------------------------------
# - cfg = KickbaseApiConfig.from_env(rps=3.0)
# - client = KickbaseClient(cfg)
# - token = client.authenticate()
# - rows = client.get_market_value_history(...)
# ------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import os
import time
from typing import Any

import requests


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class KickbaseApiError(RuntimeError):
    """Basisfehler fuer Kickbase-API-Aufrufe."""


class KickbaseAuthError(KickbaseApiError):
    """Fehler fuer fehlgeschlagene Authentifizierung."""


@dataclass(frozen=True)
class KickbaseApiConfig:
    base_url: str
    email: str
    password: str
    auth_path: str
    auth_email_field: str
    auth_password_field: str
    config_path: str
    matchdays_path: str
    event_types_path: str
    market_value_history_path: str
    market_value_history_fallback_paths: tuple[str, ...]
    performance_path: str
    performance_fallback_paths: tuple[str, ...]
    playercenter_path: str
    team_profile_path: str
    transfers_path: str
    user_agent: str
    timeout_seconds: float
    max_retries: int
    backoff_seconds: float
    rps: float

    @classmethod
    def from_env(cls, *, rps: float | None = None) -> "KickbaseApiConfig":
        configured_rps = rps if rps is not None else float(os.getenv("KICKBASE_ETL_RPS", "3"))
        market_value_fallbacks = _split_paths(
            os.getenv(
                "KICKBASE_HISTORY_MARKET_VALUE_FALLBACK_PATHS",
                "/v4/players/{player_id}/market-value,/v4/players/{player_id}/marketvalue",
            )
        )
        performance_fallbacks = _split_paths(
            os.getenv(
                "KICKBASE_HISTORY_PERFORMANCE_FALLBACK_PATHS",
                "/v4/players/{player_id}/performance",
            )
        )

        legacy_market = os.getenv("KICKBASE_PLAYER_MARKET_VALUE_HISTORY_PATH", "").strip()
        if legacy_market:
            market_value_fallbacks = tuple([legacy_market, *market_value_fallbacks])

        legacy_performance = os.getenv("KICKBASE_PLAYER_PERFORMANCE_PATH", "").strip()
        if legacy_performance:
            performance_fallbacks = tuple([legacy_performance, *performance_fallbacks])

        return cls(
            base_url=os.getenv("KICKBASE_BASE_URL", "https://api.kickbase.com").rstrip("/"),
            email=os.getenv("KICKBASE_EMAIL", "").strip(),
            password=os.getenv("KICKBASE_PASSWORD", "").strip(),
            auth_path=os.getenv("KICKBASE_AUTH_PATH", "/v4/user/login"),
            auth_email_field=os.getenv("KICKBASE_AUTH_EMAIL_FIELD", "em"),
            auth_password_field=os.getenv("KICKBASE_AUTH_PASSWORD_FIELD", "pass"),
            config_path=os.getenv("KICKBASE_CONFIG_PATH", "/v4/config"),
            matchdays_path=os.getenv(
                "KICKBASE_MATCHDAYS_PATH", "/v4/competitions/{competition_id}/matchdays"
            ),
            event_types_path=os.getenv("KICKBASE_EVENT_TYPES_PATH", "/v4/live/eventtypes"),
            market_value_history_path=os.getenv(
                "KICKBASE_HISTORY_MARKET_VALUE_PATH",
                "/v4/competitions/{competition_id}/players/{player_id}/marketvalue/{timeframe_days}",
            ),
            market_value_history_fallback_paths=market_value_fallbacks,
            performance_path=os.getenv(
                "KICKBASE_HISTORY_PERFORMANCE_PATH",
                "/v4/competitions/{competition_id}/players/{player_id}/performance",
            ),
            performance_fallback_paths=performance_fallbacks,
            playercenter_path=os.getenv(
                "KICKBASE_HISTORY_PLAYERCENTER_PATH",
                "/v4/competitions/{competition_id}/playercenter/{player_id}",
            ),
            team_profile_path=os.getenv(
                "KICKBASE_TEAM_PROFILE_PATH",
                "/v4/leagues/{league_id}/teams/{team_id}/teamprofile/",
            ),
            transfers_path=os.getenv(
                "KICKBASE_HISTORY_TRANSFERS_PATH",
                "/v4/leagues/{league_id}/players/{player_id}/transfers",
            ),
            user_agent=os.getenv("KICKBASE_USER_AGENT", "okhttp/4.11.0"),
            timeout_seconds=float(os.getenv("KICKBASE_TIMEOUT_SECONDS", "20")),
            max_retries=int(os.getenv("KICKBASE_MAX_RETRIES", "2")),
            backoff_seconds=float(os.getenv("KICKBASE_BACKOFF_SECONDS", "1")),
            rps=max(configured_rps, 0.1),
        )


class KickbaseClient:
    def __init__(self, config: KickbaseApiConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": self.config.user_agent,
            }
        )
        self._last_auth_payload: dict[str, Any] | None = None
        self._min_interval_seconds = 1.0 / self.config.rps
        self._next_request_at = 0.0

    def authenticate(self) -> str:
        payload: dict[str, Any] = {
            self.config.auth_email_field: self.config.email,
            self.config.auth_password_field: self.config.password,
        }
        if self.config.auth_email_field == "em" and self.config.auth_password_field == "pass":
            payload["loy"] = False
            payload["rep"] = {}

        auth_payload = self._request_json("POST", self.config.auth_path, payload=payload)
        if not isinstance(auth_payload, dict):
            raise KickbaseAuthError("Authentication response is not an object")

        self._last_auth_payload = auth_payload

        token_candidates: list[Any] = [
            auth_payload.get("token"),
            auth_payload.get("access_token"),
            auth_payload.get("jwt"),
            auth_payload.get("tkn"),
            auth_payload.get("chttkn"),
        ]

        nested = auth_payload.get("data")
        if isinstance(nested, dict):
            token_candidates.extend(
                [
                    nested.get("token"),
                    nested.get("access_token"),
                    nested.get("jwt"),
                    nested.get("tkn"),
                ]
            )

        for candidate in token_candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        raise KickbaseAuthError("Authentication response did not contain a token")

    def discover_competition_id(
        self,
        *,
        token: str,
        league_id: str | None,
        competition_name: str | None,
    ) -> int | None:
        # 1) aus Auth-Payload (srvl) ableiten
        auth_payload = self._last_auth_payload
        if isinstance(auth_payload, dict):
            leagues = auth_payload.get("srvl")
            if isinstance(leagues, list):
                for league in leagues:
                    if not isinstance(league, dict):
                        continue
                    if league_id and str(league.get("id", "")).strip() != str(league_id).strip():
                        continue
                    competition = league.get("cpi") or league.get("competitionId") or league.get(
                        "competition_id"
                    )
                    competition_id = _to_int(competition)
                    if competition_id is not None:
                        return competition_id

        # 2) aus /v4/config ableiten
        config_payload = self.get_config(token)
        candidates: list[int] = []

        if isinstance(config_payload, dict):
            direct_candidate = _to_int(
                config_payload.get("competitionId")
                or config_payload.get("competition_id")
                or config_payload.get("cpi")
            )
            if direct_candidate is not None:
                candidates.append(direct_candidate)

            for key in ("competitions", "it", "data", "items"):
                values = config_payload.get(key)
                if isinstance(values, list):
                    for item in values:
                        if not isinstance(item, dict):
                            continue
                        item_name = _to_text(item.get("name") or item.get("n") or item.get("title"))
                        if competition_name and competition_name.lower() not in item_name.lower():
                            continue
                        candidate = _to_int(
                            item.get("id")
                            or item.get("competitionId")
                            or item.get("competition_id")
                            or item.get("cpi")
                        )
                        if candidate is not None:
                            candidates.append(candidate)

        return candidates[0] if candidates else None

    def discover_league_id(self) -> int | None:
        auth_payload = self._last_auth_payload
        if not isinstance(auth_payload, dict):
            return None

        leagues = auth_payload.get("srvl")
        if not isinstance(leagues, list):
            return None

        for league in leagues:
            if not isinstance(league, dict):
                continue
            candidate = _to_int(league.get("id"))
            if candidate is not None:
                return candidate
        return None

    def get_config(self, token: str) -> dict[str, Any]:
        payload = self._request_json("GET", self.config.config_path, token=token)
        return payload if isinstance(payload, dict) else {}

    def get_matchdays(self, token: str, competition_id: int) -> dict[str, Any] | list[Any]:
        path = self.config.matchdays_path.format(competition_id=competition_id)
        return self._request_json("GET", path, token=token)

    def get_event_types(self, token: str) -> dict[str, Any] | list[Any]:
        return self._request_json("GET", self.config.event_types_path, token=token)

    def get_market_value_history(
        self,
        token: str,
        competition_id: int,
        player_id: int,
        timeframe_days: int,
    ) -> dict[str, Any] | list[Any]:
        first_payload: dict[str, Any] | list[Any] | None = None
        timeframe_candidates = [int(timeframe_days)]
        if timeframe_days > 365:
            timeframe_candidates.append(365)

        for timeframe_candidate in timeframe_candidates:
            paths = _resolve_candidate_paths(
                self.config.market_value_history_path,
                self.config.market_value_history_fallback_paths,
                {
                    "competition_id": competition_id,
                    "player_id": player_id,
                    "timeframe_days": timeframe_candidate,
                },
            )
            payload = self._request_first_non_empty("GET", paths, token=token)
            if first_payload is None:
                first_payload = payload
            if _payload_has_rows(payload):
                return payload

        return first_payload or {}

    def get_performance(
        self,
        token: str,
        competition_id: int,
        player_id: int,
    ) -> dict[str, Any] | list[Any]:
        paths = _resolve_candidate_paths(
            self.config.performance_path,
            self.config.performance_fallback_paths,
            {
                "competition_id": competition_id,
                "player_id": player_id,
            },
        )
        return self._request_first_non_empty("GET", paths, token=token)

    def get_playercenter(
        self,
        token: str,
        competition_id: int,
        player_id: int,
        day_number: int,
    ) -> dict[str, Any] | list[Any]:
        path = self.config.playercenter_path.format(
            competition_id=competition_id,
            player_id=player_id,
        )
        return self._request_json(
            "GET",
            path,
            token=token,
            params={"dayNumber": day_number},
        )

    def get_team_profile(
        self,
        token: str,
        league_id: int | str,
        team_id: int,
    ) -> dict[str, Any] | list[Any]:
        path = self.config.team_profile_path.format(league_id=league_id, team_id=team_id)
        return self._request_json("GET", path, token=token)

    def get_player_transfers(
        self,
        token: str,
        league_id: str,
        player_id: int,
    ) -> dict[str, Any] | list[Any]:
        path = self.config.transfers_path.format(league_id=league_id, player_id=player_id)
        return self._request_json("GET", path, token=token)

    def _request_first_non_empty(
        self,
        method: str,
        paths: list[str],
        *,
        token: str | None = None,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        first_payload: dict[str, Any] | list[Any] | None = None
        last_error: KickbaseApiError | None = None
        for path in paths:
            try:
                response_payload = self._request_json(
                    method,
                    path,
                    token=token,
                    payload=payload,
                    params=params,
                )
            except KickbaseApiError as exc:
                last_error = exc
                continue
            if first_payload is None:
                first_payload = response_payload
            if _payload_has_rows(response_payload):
                return response_payload
        if first_payload is not None:
            return first_payload
        if last_error is not None:
            raise last_error
        return {}

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        url = _join_url(self.config.base_url, path)
        headers: dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        for attempt in range(self.config.max_retries + 1):
            self._apply_rate_limit()
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                json=payload,
                params=params,
                timeout=self.config.timeout_seconds,
            )

            if 200 <= response.status_code < 300:
                if not response.text.strip():
                    return {}
                try:
                    parsed = response.json()
                except ValueError as exc:
                    raise KickbaseApiError(
                        f"Invalid JSON for {method} {url}: {response.text[:250]}"
                    ) from exc
                if isinstance(parsed, (dict, list)):
                    return parsed
                raise KickbaseApiError(
                    f"Unexpected payload type for {method} {url}: {type(parsed)!r}"
                )

            if response.status_code in {401, 403}:
                raise KickbaseAuthError(
                    f"Authentication failed for {method} {url} (status {response.status_code})"
                )

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.config.max_retries:
                sleep_seconds = self.config.backoff_seconds * (2**attempt)
                time.sleep(sleep_seconds)
                continue

            raise KickbaseApiError(
                f"Request failed for {method} {url} (status {response.status_code}): {response.text[:250]}"
            )

        raise KickbaseApiError(f"Request failed for {method} {url}")

    def _apply_rate_limit(self) -> None:
        now = time.monotonic()
        if now < self._next_request_at:
            time.sleep(self._next_request_at - now)
        self._next_request_at = time.monotonic() + self._min_interval_seconds


def _join_url(base_url: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _split_paths(raw: str) -> tuple[str, ...]:
    parts = []
    for chunk in raw.split(","):
        item = chunk.strip()
        if item:
            parts.append(item)
    return tuple(parts)


def _resolve_candidate_paths(
    primary: str,
    fallbacks: tuple[str, ...],
    format_values: dict[str, Any],
) -> list[str]:
    candidates: list[str] = []
    for template in (primary, *fallbacks):
        try:
            path = template.format(**format_values)
        except KeyError:
            continue
        if path and path not in candidates:
            candidates.append(path)
    return candidates


def _payload_has_rows(payload: dict[str, Any] | list[Any]) -> bool:
    if isinstance(payload, list):
        return any(isinstance(item, dict) for item in payload)
    if isinstance(payload, dict):
        for key in ("it", "data", "items", "rows", "result", "players", "history", "ph"):
            value = payload.get(key)
            if isinstance(value, list) and value:
                return True
    return False
