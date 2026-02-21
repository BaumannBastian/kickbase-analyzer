# ------------------------------------
# test_demo_ingestion.py
#
# Tests fuer Demo-Ingestion und den allgemeinen CLI-Runner.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_demo_ingestion -v
# ------------------------------------

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
import unittest

from local_ingestion.core.bronze_writer import DATASETS, run_demo_ingestion
from local_ingestion.runners.run_ingestion import main


class DemoIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.demo_dir = self.repo_root / "demo" / "data"

    def test_run_demo_ingestion_writes_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            summary = run_demo_ingestion(
                self.demo_dir,
                out_dir,
                now=datetime(2026, 2, 21, 12, 0, 0),
                source_version="demo-test",
            )

            self.assertEqual(summary["status"], "success")
            self.assertEqual(summary["mode"], "demo")
            self.assertEqual(len(summary["files_written"]), len(DATASETS))
            self.assertEqual(summary["rows_written"], 6)

            for filename in summary["files_written"]:
                self.assertTrue((out_dir / filename).exists())

    def test_cli_demo_mode_runs_successfully(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            exit_code = main(
                [
                    "--mode",
                    "demo",
                    "--out-dir",
                    str(out_dir),
                    "--timestamp",
                    "2026-02-21T120000Z",
                ]
            )
            self.assertEqual(exit_code, 0)
            self.assertTrue((out_dir / "ingestion_runs.ndjson").exists())


if __name__ == "__main__":
    unittest.main()
