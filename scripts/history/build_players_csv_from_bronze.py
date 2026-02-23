# ------------------------------------
# build_players_csv_from_bronze.py
#
# Dieses Skript erzeugt aus dem neuesten Kickbase-Bronze-Snapshot
# eine ETL-Driver-CSV fuer `src.etl_history`.
#
# Outputs
# ------------------------------------
# 1) CSV-Datei mit Player-Master-Spalten fuer den History-Load.
#
# Usage
# ------------------------------------
# - python -m scripts.history.build_players_csv_from_bronze --input-dir data/bronze --output in/players_all.csv
# - python -m scripts.history.build_players_csv_from_bronze --limit 50 --output in/players_50.csv
# ------------------------------------

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path
from typing import Any


FIELDNAMES = [
    "player_uid",
    "kb_player_id",
    "player_name",
    "kickbase_team_id",
    "team_code",
    "team_name",
    "position",
    "league_key",
    "competition_id",
    "ligainsider_player_slug",
    "ligainsider_player_id",
    "birthdate",
    "image_url",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Erstellt History-Driver-CSV aus Kickbase-Bronze-Snapshot.")
    parser.add_argument("--input-dir", default="data/bronze", help="Bronze-Verzeichnis")
    parser.add_argument("--output", default="in/players_all.csv", help="Output-CSV Pfad")
    parser.add_argument("--league-key", default="bundesliga_1")
    parser.add_argument("--competition-id", type=int, default=1)
    parser.add_argument("--limit", type=int, default=0, help="Optionales Zeilenlimit (0 = alle)")
    return parser.parse_args(argv)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _latest_kickbase_snapshot(input_dir: Path) -> Path:
    pattern = str(input_dir / "kickbase_player_snapshot_*.ndjson")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"Kein Kickbase-Bronze-Snapshot gefunden unter {pattern}")
    return Path(files[-1])


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    snapshot_path = _latest_kickbase_snapshot(Path(args.input_dir))

    rows_by_kb_id: dict[int, dict[str, Any]] = {}
    with snapshot_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            row = json.loads(raw)
            if not isinstance(row, dict):
                continue

            kb_player_id = _to_int(row.get("kickbase_player_id"))
            if kb_player_id is None:
                continue

            if kb_player_id in rows_by_kb_id:
                continue

            rows_by_kb_id[kb_player_id] = {
                "player_uid": "",
                "kb_player_id": kb_player_id,
                "player_name": _to_text(row.get("player_name")),
                "kickbase_team_id": _to_int(row.get("team_id")) or "",
                "team_code": "",
                "team_name": "",
                "position": _to_text(row.get("position")),
                "league_key": args.league_key,
                "competition_id": args.competition_id,
                "ligainsider_player_slug": "",
                "ligainsider_player_id": "",
                "birthdate": "",
                "image_url": "",
            }

    rows = sorted(rows_by_kb_id.values(), key=lambda row: str(row["player_name"]).lower())
    if args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(
        json.dumps(
            {
                "input_snapshot": snapshot_path.as_posix(),
                "output_csv": output_path.as_posix(),
                "rows_written": len(rows),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
