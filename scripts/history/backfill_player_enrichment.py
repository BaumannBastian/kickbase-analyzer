# ------------------------------------
# backfill_player_enrichment.py
#
# Backfill-Skript fuer dim_player:
# - fehlende Birthdates / LigaInsider-IDs / Profile-URLs nachziehen
# - fehlende lokale Bilddateien (aus bestehendem Blob oder neuem Download) erzeugen
# - offene Problemfaelle in JSONL reporten
#
# Usage
# ------------------------------------
# - python -m scripts.history.backfill_player_enrichment --env-file .env
# - python -m scripts.history.backfill_player_enrichment --env-file .env --limit 100
# - python -m scripts.history.backfill_player_enrichment --env-file .env --skip-image-download
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args: object, **_kwargs: object) -> bool:  # type: ignore[override]
        return False

from src.db import DbConfig, get_connection, upsert_dim_players
from src.etl_history import (
    ImageLoadResult,
    PlayerMaster,
    load_latest_ligainsider_lookup,
    load_player_image_blob,
    persist_player_image_file,
    resolve_player_enrichment,
)


LOGGER = logging.getLogger("backfill_player_enrichment")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill fehlender dim_player Enrichment-Felder.")
    parser.add_argument("--env-file", default=".env", help="Pfad zur .env Datei")
    parser.add_argument("--bronze-dir", default="data/bronze", help="Bronze-Verzeichnis fuer LigaInsider Snapshot")
    parser.add_argument("--report-dir", default="out/reports", help="Output-Verzeichnis fuer Problemreport")
    parser.add_argument("--limit", type=int, default=0, help="Maximale Anzahl zu bearbeitender Spieler (0 = alle)")
    parser.add_argument("--league-key", default=os.getenv("KICKBASE_LEAGUE_KEY", "bundesliga_1"))
    parser.add_argument("--competition-id", type=int, default=1)
    parser.add_argument("--skip-image-download", action="store_true")
    parser.add_argument("--image-timeout-seconds", type=float, default=float(os.getenv("IMAGE_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--image-max-bytes", type=int, default=int(os.getenv("IMAGE_MAX_BYTES", str(5 * 1024 * 1024))))
    parser.add_argument("--image-retries", type=int, default=int(os.getenv("IMAGE_RETRIES", "2")))
    parser.add_argument("--image-backoff-seconds", type=float, default=float(os.getenv("IMAGE_BACKOFF_SECONDS", "1")))
    parser.add_argument("--image-cache-dir", default=os.getenv("IMAGE_CACHE_DIR", ".cache/player_images"))
    parser.add_argument("--image-store-dir", default=os.getenv("IMAGE_STORE_DIR", "data/history/player_images"))
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def fetch_targets(conn: object, *, limit: int) -> list[dict[str, object]]:
    query = """
        SELECT
            d.player_uid,
            d.kb_player_id,
            d.player_name,
            d.team_uid,
            d.player_position,
            d.player_birthdate,
            d.ligainsider_player_id,
            d.ligainsider_player_slug,
            d.ligainsider_name,
            d.ligainsider_profile_url,
            d.image_sha256,
            d.image_mime,
            d.image_blob,
            d.image_local_path,
            t.kickbase_team_id,
            t.team_code,
            t.team_name
        FROM dim_player AS d
        LEFT JOIN dim_team AS t
          ON t.team_uid = d.team_uid
        WHERE
            d.player_birthdate IS NULL
            OR d.ligainsider_player_id IS NULL
            OR d.ligainsider_profile_url IS NULL
            OR d.image_local_path IS NULL
        ORDER BY d.player_uid
    """
    params: tuple[object, ...] = ()
    if limit > 0:
        query += " LIMIT %s"
        params = (int(limit),)

    out: list[dict[str, object]] = []
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [col.name for col in cur.description]
        for row in cur.fetchall():
            out.append(dict(zip(columns, row, strict=False)))
    return out


def build_player_master(row: dict[str, object], *, league_key: str, competition_id: int) -> PlayerMaster:
    return PlayerMaster(
        player_uid=str(row.get("player_uid") or "").strip() or None,
        kb_player_id=_to_int_or_none(row.get("kb_player_id")),
        player_name=str(row.get("player_name") or "").strip(),
        kickbase_team_id=_to_int_or_none(row.get("kickbase_team_id")),
        team_code=_to_text_or_none(row.get("team_code")),
        team_name=_to_text_or_none(row.get("team_name")),
        position=_to_text_or_none(row.get("player_position")),
        league_key=league_key,
        competition_id=competition_id,
        ligainsider_player_slug=_to_text_or_none(row.get("ligainsider_player_slug")),
        ligainsider_player_id=_to_int_or_none(row.get("ligainsider_player_id")),
        birthdate=row.get("player_birthdate"),
        image_url=None,
    )


def ensure_local_image_file(
    *,
    player_uid: str,
    row: dict[str, object],
    image_store_dir: Path,
) -> str | None:
    image_blob = row.get("image_blob")
    image_mime = _to_text_or_none(row.get("image_mime"))
    if image_blob is None or image_mime is None:
        return _to_text_or_none(row.get("image_local_path"))
    return persist_player_image_file(
        player_uid=player_uid,
        image_blob=bytes(image_blob),
        image_mime=image_mime,
        output_dir=image_store_dir,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_dotenv(args.env_file)
    configure_logging(args.log_level)

    db_config = DbConfig.from_env()
    ligainsider_lookup = load_latest_ligainsider_lookup(Path(args.bronze_dir))
    ligainsider_profile_cache: dict[str, dict[str, object]] = {}
    ligainsider_session = requests.Session()
    ligainsider_timeout_seconds = float(os.getenv("LIGAINSIDER_TIMEOUT_SECONDS", "15"))
    image_session = requests.Session()
    image_cache_dir = Path(args.image_cache_dir)
    image_store_dir = Path(args.image_store_dir)

    summary: dict[str, int] = {
        "players_targeted": 0,
        "players_updated": 0,
        "birthdate_filled": 0,
        "ligainsider_id_filled": 0,
        "profile_url_filled": 0,
        "image_path_filled": 0,
        "image_updated": 0,
        "issues_open": 0,
    }

    issues: list[dict[str, object]] = []

    with get_connection(db_config) as conn:
        targets = fetch_targets(conn, limit=args.limit)
        summary["players_targeted"] = len(targets)

        for row in targets:
            player = build_player_master(
                row,
                league_key=args.league_key,
                competition_id=int(args.competition_id),
            )
            player_uid = str(player.player_uid or "").strip()
            if not player_uid:
                continue

            existing = SimpleNamespace(birthdate=row.get("player_birthdate"))
            enrichment = resolve_player_enrichment(
                player=player,
                existing_identity=existing,
                ligainsider_lookup=ligainsider_lookup,
                ligainsider_profile_cache=ligainsider_profile_cache,
                ligainsider_session=ligainsider_session,
                ligainsider_timeout_seconds=ligainsider_timeout_seconds,
            )

            image_result: ImageLoadResult = ImageLoadResult(None, None, None, "skipped")
            image_local_path = _to_text_or_none(row.get("image_local_path"))
            if args.skip_image_download:
                image_local_path = image_local_path or ensure_local_image_file(
                    player_uid=player_uid,
                    row=row,
                    image_store_dir=image_store_dir,
                )
            else:
                image_result = load_player_image_blob(
                    session=image_session,
                    player_uid=player_uid,
                    image_url=_to_text_or_none(enrichment.get("image_url")),
                    existing_sha256=_to_text_or_none(row.get("image_sha256")),
                    cache_dir=image_cache_dir,
                    timeout_seconds=args.image_timeout_seconds,
                    max_bytes=args.image_max_bytes,
                    retries=args.image_retries,
                    backoff_seconds=args.image_backoff_seconds,
                )
                if image_result.image_blob is not None and image_result.image_mime is not None:
                    image_local_path = persist_player_image_file(
                        player_uid=player_uid,
                        image_blob=image_result.image_blob,
                        image_mime=image_result.image_mime,
                        output_dir=image_store_dir,
                    )
                elif not image_local_path:
                    image_local_path = ensure_local_image_file(
                        player_uid=player_uid,
                        row=row,
                        image_store_dir=image_store_dir,
                    )

            before_birthdate = row.get("player_birthdate")
            before_li_id = row.get("ligainsider_player_id")
            before_profile = row.get("ligainsider_profile_url")
            before_image_path = row.get("image_local_path")

            upsert_dim_players(
                conn,
                [
                    {
                        "player_uid": player_uid,
                        "kb_player_id": player.kb_player_id,
                        "player_name": player.player_name,
                        "team_uid": _to_text_or_none(row.get("team_uid")),
                        "ligainsider_player_id": enrichment.get("ligainsider_player_id"),
                        "ligainsider_player_slug": enrichment.get("ligainsider_player_slug"),
                        "ligainsider_name": enrichment.get("ligainsider_name") or row.get("ligainsider_name"),
                        "ligainsider_profile_url": enrichment.get("ligainsider_profile_url"),
                        "player_position": player.position,
                        "player_birthdate": enrichment.get("birthdate"),
                        "image_blob": image_result.image_blob,
                        "image_mime": image_result.image_mime,
                        "image_sha256": image_result.image_sha256,
                        "image_local_path": image_local_path,
                    }
                ],
            )

            summary["players_updated"] += 1
            if before_birthdate is None and enrichment.get("birthdate") is not None:
                summary["birthdate_filled"] += 1
            if before_li_id is None and enrichment.get("ligainsider_player_id") is not None:
                summary["ligainsider_id_filled"] += 1
            if before_profile is None and enrichment.get("ligainsider_profile_url") is not None:
                summary["profile_url_filled"] += 1
            if not _to_text_or_none(before_image_path) and image_local_path:
                summary["image_path_filled"] += 1
            if image_result.status in {"updated", "updated_from_cache"}:
                summary["image_updated"] += 1

            missing_after: list[str] = []
            if enrichment.get("birthdate") is None and before_birthdate is None:
                missing_after.append("player_birthdate")
            if enrichment.get("ligainsider_player_id") is None and before_li_id is None:
                missing_after.append("ligainsider_player_id")
            if enrichment.get("ligainsider_profile_url") is None and before_profile is None:
                missing_after.append("ligainsider_profile_url")
            if not image_local_path:
                missing_after.append("image_local_path")

            if missing_after:
                issues.append(
                    {
                        "player_uid": player_uid,
                        "player_name": player.player_name,
                        "kb_player_id": player.kb_player_id,
                        "missing_fields": missing_after,
                        "ligainsider_player_slug": enrichment.get("ligainsider_player_slug"),
                        "ligainsider_player_id": enrichment.get("ligainsider_player_id"),
                        "ligainsider_profile_url": enrichment.get("ligainsider_profile_url"),
                        "image_status": image_result.status,
                    }
                )

        conn.commit()

    summary["issues_open"] = len(issues)

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H%M%SZ")
    report_path = report_dir / f"player_enrichment_issues_{report_ts}.jsonl"
    with report_path.open("w", encoding="utf-8") as handle:
        for issue in issues:
            handle.write(json.dumps(issue, ensure_ascii=True) + "\n")

    print(
        json.dumps(
            {
                "summary": summary,
                "issues_report": report_path.as_posix(),
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _to_int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


if __name__ == "__main__":
    raise SystemExit(main())

