# ------------------------------------
# test_powerbi_desktop_assets.py
#
# Tests fuer Export lokaler Power-BI-Desktop-Assets (M/DAX/TMDL).
#
# Usage
# ------------------------------------
# - python3 -m unittest tests.test_powerbi_desktop_assets -v
# ------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.powerbi_desktop.export_desktop_assets import main


class PowerBIDesktopAssetsTests(unittest.TestCase):
    def test_export_writes_all_files_with_replaced_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_root = Path(tmp) / "out"
            exit_code = main(
                [
                    "--project",
                    "kickbase-analyzer",
                    "--raw-dataset",
                    "kickbase_raw",
                    "--core-dataset",
                    "kickbase_core",
                    "--marts-dataset",
                    "kickbase_marts",
                    "--output-root",
                    str(out_root),
                    "--no-timestamp-dir",
                ]
            )
            self.assertEqual(exit_code, 0)

            pack_dir = out_root / "desktop_pack"
            pq_path = pack_dir / "bigquery_marts_queries.pq"
            dax_path = pack_dir / "measures.dax"
            tmdl_path = pack_dir / "model.tmdl"
            txt_path = pack_dir / "README.txt"

            self.assertTrue(pq_path.exists())
            self.assertTrue(dax_path.exists())
            self.assertTrue(tmdl_path.exists())
            self.assertTrue(txt_path.exists())

            pq_text = pq_path.read_text(encoding="utf-8")
            self.assertIn("kickbase-analyzer", pq_text)
            self.assertIn("kickbase_marts", pq_text)


if __name__ == "__main__":
    unittest.main()

