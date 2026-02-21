# ------------------------------------
# export_desktop_assets.py
#
# Dieses Skript erstellt ein lokales Power-BI-Desktop-Asset-Pack
# aus versionierten Templates (M, DAX, TMDL) fuer BigQuery MARTS.
# Platzhalter fuer Projekt/Datasets werden beim Export ersetzt.
#
# Outputs
# ------------------------------------
# 1) dashboards/powerbi/local/desktop_pack_<timestamp>/bigquery_marts_queries.pq
# 2) dashboards/powerbi/local/desktop_pack_<timestamp>/measures.dax
# 3) dashboards/powerbi/local/desktop_pack_<timestamp>/model.tmdl
# 4) dashboards/powerbi/local/desktop_pack_<timestamp>/README.txt
#
# Usage
# ------------------------------------
# - python -m scripts.powerbi_desktop.export_desktop_assets
# - python -m scripts.powerbi_desktop.export_desktop_assets --project kickbase-analyzer
# - python -m scripts.powerbi_desktop.export_desktop_assets --no-timestamp-dir
# ------------------------------------

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Sequence


DEFAULT_TEMPLATE_DIR = Path("dashboards/powerbi/templates")
DEFAULT_OUTPUT_ROOT = Path("dashboards/powerbi/local")

TEMPLATE_TO_OUTPUT = {
    "bigquery_marts_queries.pq": "bigquery_marts_queries.pq",
    "measures.dax": "measures.dax",
    "model.tmdl": "model.tmdl",
}


def now_compact_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H%M%SZ")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Power BI Desktop assets from local templates.")
    parser.add_argument("--project", default=os.environ.get("BQ_PROJECT_ID", "kickbase-analyzer"))
    parser.add_argument("--raw-dataset", default="kickbase_raw")
    parser.add_argument("--core-dataset", default="kickbase_core")
    parser.add_argument("--marts-dataset", default="kickbase_marts")
    parser.add_argument("--template-dir", type=Path, default=DEFAULT_TEMPLATE_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-timestamp-dir", action="store_true")
    return parser.parse_args(argv)


def render_template(text: str, *, project: str, raw: str, core: str, marts: str) -> str:
    return (
        text.replace("{project}", project)
        .replace("{raw}", raw)
        .replace("{core}", core)
        .replace("{marts}", marts)
    )


def build_readme_text(*, project: str, marts: str, out_dir: Path) -> str:
    return "\n".join(
        [
            "Power BI Desktop Asset Pack",
            "",
            f"Project: {project}",
            f"MARTS Dataset: {marts}",
            "",
            "Verwendung:",
            "1) Power BI Desktop oeffnen.",
            "2) Neue leere Abfrage anlegen und Inhalt aus bigquery_marts_queries.pq uebernehmen.",
            "3) Optional: DAX-Measures aus measures.dax in die jeweiligen Tabellen einpflegen.",
            "4) Optional: TMDL-Template aus model.tmdl fuer PBIP/TMDL-Workflow nutzen.",
            "",
            "Hinweis:",
            "Dieser Ordner ist lokal und standardmaessig nicht fuer Git-Commits vorgesehen.",
            "",
            f"Output Directory: {out_dir}",
        ]
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    if args.no_timestamp_dir:
        out_dir = args.output_root / "desktop_pack"
    else:
        out_dir = args.output_root / f"desktop_pack_{now_compact_utc()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    files_written: list[str] = []
    for template_name, output_name in TEMPLATE_TO_OUTPUT.items():
        source_path = args.template_dir / template_name
        if not source_path.exists():
            raise FileNotFoundError(f"Missing template file: {source_path}")
        raw_text = source_path.read_text(encoding="utf-8")
        rendered = render_template(
            raw_text,
            project=args.project,
            raw=args.raw_dataset,
            core=args.core_dataset,
            marts=args.marts_dataset,
        )
        output_path = out_dir / output_name
        output_path.write_text(rendered, encoding="utf-8")
        files_written.append(str(output_path))

    readme_path = out_dir / "README.txt"
    readme_path.write_text(
        build_readme_text(project=args.project, marts=args.marts_dataset, out_dir=out_dir),
        encoding="utf-8",
    )
    files_written.append(str(readme_path))

    summary = {
        "status": "success",
        "project": args.project,
        "raw_dataset": args.raw_dataset,
        "core_dataset": args.core_dataset,
        "marts_dataset": args.marts_dataset,
        "output_dir": str(out_dir),
        "files_written": files_written,
    }
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
