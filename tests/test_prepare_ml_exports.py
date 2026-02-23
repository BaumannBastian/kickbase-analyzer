# ------------------------------------
# test_prepare_ml_exports.py
#
# Tests fuer den ML-RAW Export aus data/ml_runs/<run_ts>.
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_prepare_ml_exports -v
# ------------------------------------

from __future__ import annotations

import csv
import json
from pathlib import Path
import tempfile
import unittest

from bigquery.raw_load.prepare_ml_exports import run_prepare_ml_exports


class PrepareMlExportsTests(unittest.TestCase):
    def test_prepare_ml_exports_writes_jsonl_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ml_runs_dir = root / "ml_runs"
            run_dir = ml_runs_dir / "2026-02-23T120000Z"
            out_dir = root / "warehouse" / "raw_ml"
            run_dir.mkdir(parents=True, exist_ok=True)

            (run_dir / "run_summary.json").write_text(
                json.dumps(
                    {
                        "history_players": 50,
                        "history_rows": 2000,
                        "live_rows": 320,
                        "sklearn_summary": {"metrics_mean": {"metric_points_total_rmse": 85.0}},
                        "torch_summary": {"metrics_mean": {"metric_points_total_rmse": 83.0}},
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "champion_selection.json").write_text(
                json.dumps(
                    {
                        "champion_model": "torch",
                        "champion_reason": "lower points_total_rmse on historical CV",
                        "metric_key": "metric_points_total_rmse",
                        "sklearn_score": 85.0,
                        "torch_score": 83.0,
                    }
                ),
                encoding="utf-8",
            )

            with (run_dir / "live_predictions_champion.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["player_uid", "player_name", "expected_points_next_match"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "player_uid": "WilliOrban19921103",
                        "player_name": "Willi Orban",
                        "expected_points_next_match": "101.2",
                    }
                )

            with (run_dir / "cv_sklearn_folds.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["fold", "metric_points_total_rmse"],
                )
                writer.writeheader()
                writer.writerow({"fold": "1", "metric_points_total_rmse": "85.0"})

            summary = run_prepare_ml_exports(
                ml_runs_dir=ml_runs_dir,
                output_dir=out_dir,
                run_dir=run_dir,
            )

            self.assertEqual(summary["status"], "success")
            self.assertTrue((out_dir / "ml_live_predictions.jsonl").exists())
            self.assertTrue((out_dir / "ml_cv_fold_metrics.jsonl").exists())
            self.assertTrue((out_dir / "ml_champion_selection.jsonl").exists())
            self.assertTrue((out_dir / "ml_run_summary.jsonl").exists())
            self.assertTrue((out_dir / "manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
