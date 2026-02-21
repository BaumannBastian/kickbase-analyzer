# ------------------------------------
# test_jobs_pipeline.py
#
# Integrationstest fuer den lokalen Bronze->Silver->Gold->MARTS Ablauf
# auf Basis der Demo-Daten.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.pipeline.test_jobs_pipeline -v
# ------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from bigquery.marts.build_marts_local import run_build_marts_local
from databricks.jobs.bronze_ingest.run_bronze_ingest import run_bronze_ingest
from databricks.jobs.gold_features.run_gold_features import run_gold_features
from databricks.jobs.silver_sync.run_silver_sync import run_silver_sync
from local_ingestion.core.bronze_writer import run_demo_ingestion


class JobsPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.demo_dir = self.repo_root / "demo" / "data"

    def test_full_pipeline_produces_marts_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bronze_dir = root / "bronze"
            lakehouse_bronze_dir = root / "lakehouse" / "bronze"
            lakehouse_silver_dir = root / "lakehouse" / "silver"
            lakehouse_gold_dir = root / "lakehouse" / "gold"
            marts_dir = root / "marts"

            demo_summary = run_demo_ingestion(
                self.demo_dir,
                bronze_dir,
                source_version="demo-test",
            )
            self.assertEqual(demo_summary["status"], "success")

            bronze_summary = run_bronze_ingest(bronze_dir, lakehouse_bronze_dir)
            silver_summary = run_silver_sync(lakehouse_bronze_dir, lakehouse_silver_dir)
            gold_summary = run_gold_features(lakehouse_silver_dir, lakehouse_gold_dir)
            marts_summary = run_build_marts_local(lakehouse_gold_dir, marts_dir)

            self.assertEqual(bronze_summary["status"], "success")
            self.assertEqual(silver_summary["status"], "success")
            self.assertEqual(gold_summary["status"], "success")
            self.assertEqual(marts_summary["status"], "success")

            leaderboard_paths = [
                path
                for path in marts_summary["files_written"]
                if "mart_player_leaderboard" in str(path)
            ]
            self.assertEqual(len(leaderboard_paths), 1)
            leaderboard_path = Path(str(leaderboard_paths[0]))
            self.assertTrue(leaderboard_path.exists())

            preview = marts_summary.get("top_player_preview", {})
            self.assertIn("player_name", preview)
            self.assertIn("expected_points_next_matchday", preview)
            self.assertIn("value_score", preview)
            self.assertEqual(preview.get("risk_method"), "monte_carlo_v1")
            self.assertGreaterEqual(int(preview.get("monte_carlo_samples", 0)), 100)


if __name__ == "__main__":
    unittest.main()
