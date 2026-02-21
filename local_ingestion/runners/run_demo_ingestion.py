# ------------------------------------
# run_demo_ingestion.py
#
# Rueckwaertskompatibler Einstiegspunkt fuer Demo-Ingestion.
# Das Modul delegiert auf den allgemeinen Runner.
#
# Usage
# ------------------------------------
# - python -m local_ingestion.runners.run_demo_ingestion
# ------------------------------------

from __future__ import annotations

from local_ingestion.runners.run_ingestion import main


if __name__ == "__main__":
    raise SystemExit(main())
