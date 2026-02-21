# ------------------------------------
# cache.py
#
# Dieses Modul stellt einen einfachen JSON-Datei-Cache bereit.
# Der Cache wird fuer private API-Requests genutzt, um redundante
# Calls zu reduzieren und lokale Runs zu stabilisieren.
#
# Outputs
# ------------------------------------
# 1) data/cache/**/*.json
#
# Usage
# ------------------------------------
# - cache = JsonFileCache(Path("data/cache"))
# - cache.get(key, ttl_seconds=300)
# - cache.set(key, payload)
# ------------------------------------

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any


class JsonFileCache:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir

    def _path_for_key(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        prefix = digest[:2]
        return self.root_dir / prefix / f"{digest}.json"

    def get(self, key: str, *, ttl_seconds: int) -> Any | None:
        path = self._path_for_key(key)
        if not path.exists():
            return None

        if ttl_seconds >= 0:
            modified_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
            age_seconds = (datetime.now(UTC) - modified_at).total_seconds()
            if age_seconds > ttl_seconds:
                return None

        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def set(self, key: str, payload: Any) -> None:
        path = self._path_for_key(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True, sort_keys=True)
