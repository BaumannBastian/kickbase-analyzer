# ------------------------------------
# test_backtesting.py
#
# Integrationstest fuer den lokalen Backtesting-Runner.
# ------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from databricks.jobs.bronze_ingest.run_bronze_ingest import run_bronze_ingest
from databricks.jobs.gold_features.run_gold_features import run_gold_features
from databricks.jobs.silver_sync.run_silver_sync import run_silver_sync
from local_ingestion.core.bronze_writer import run_demo_ingestion
from scripts.ml.run_backtesting import run_backtesting


class BacktestingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.demo_dir = self.repo_root / "demo" / "data"

    def test_backtesting_produces_summary_and_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bronze_dir = root / "bronze"
            lakehouse_bronze_dir = root / "lakehouse" / "bronze"
            lakehouse_silver_dir = root / "lakehouse" / "silver"
            lakehouse_gold_dir = root / "lakehouse" / "gold"
            backtesting_dir = root / "backtesting"

            run_demo_ingestion(self.demo_dir, bronze_dir, source_version="demo-test")
            run_bronze_ingest(bronze_dir, lakehouse_bronze_dir)
            run_silver_sync(lakehouse_bronze_dir, lakehouse_silver_dir)
            run_gold_features(lakehouse_silver_dir, lakehouse_gold_dir, mc_samples=250)

            summary = run_backtesting(lakehouse_gold_dir, lakehouse_silver_dir, backtesting_dir)
            self.assertEqual(summary["status"], "success")
            self.assertGreaterEqual(int(summary["matched_rows"]), 1)
            self.assertGreaterEqual(int(summary["summary_rows"]), 2)

            for output in summary["files_written"]:
                self.assertTrue(Path(str(output)).exists())


if __name__ == "__main__":
    unittest.main()
