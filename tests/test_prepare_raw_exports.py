# ------------------------------------
# test_prepare_raw_exports.py
#
# Tests fuer RAW-Export aus Gold-Snapshots.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_prepare_raw_exports -v
# ------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from bigquery.raw_load.prepare_raw_exports import run_prepare_raw_exports
from databricks.jobs.bronze_ingest.run_bronze_ingest import run_bronze_ingest
from databricks.jobs.gold_features.run_gold_features import run_gold_features
from databricks.jobs.silver_sync.run_silver_sync import run_silver_sync
from local_ingestion.core.bronze_writer import run_demo_ingestion


class PrepareRawExportsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.demo_dir = self.repo_root / "demo" / "data"

    def test_prepare_raw_exports_writes_jsonl_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bronze_dir = root / "bronze"
            lh_bronze = root / "lakehouse" / "bronze"
            lh_silver = root / "lakehouse" / "silver"
            lh_gold = root / "lakehouse" / "gold"
            raw_out = root / "warehouse" / "raw"

            run_demo_ingestion(self.demo_dir, bronze_dir, source_version="demo-test")
            run_bronze_ingest(bronze_dir, lh_bronze)
            run_silver_sync(lh_bronze, lh_silver)
            run_gold_features(lh_silver, lh_gold)

            summary = run_prepare_raw_exports(lh_gold, raw_out)
            self.assertEqual(summary["status"], "success")
            self.assertTrue((raw_out / "feat_player_daily.jsonl").exists())
            self.assertTrue((raw_out / "points_components_matchday.jsonl").exists())
            self.assertTrue((raw_out / "quality_metrics.jsonl").exists())
            self.assertTrue((raw_out / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
