from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

from integrity_validator import DataIntegrityValidator


class TestDataIntegrityValidator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[1]
        cls.test_db = cls.root / "data" / "test_warehouse.db"

        subprocess.run(
            [
                sys.executable,
                str(cls.root / "scripts" / "setup_sample_db.py"),
                "--db-path",
                str(cls.test_db),
            ],
            check=True,
            cwd=cls.root,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.test_db.exists():
            cls.test_db.unlink()

    def test_validator_returns_expected_report_shape(self) -> None:
        validator = DataIntegrityValidator(
            data_dir=str(self.root / "data"),
            db_path=str(self.test_db),
            schema_path=str(self.root / "data" / "expected_schema.json"),
            sql_checks_path=str(self.root / "sql" / "consistency_checks.sql"),
        )
        report = validator.run()

        self.assertIn("summary", report)
        self.assertIn("issues", report)
        self.assertGreater(report["summary"]["total_issues"], 0)

        categories = {issue["category"] for issue in report["issues"]}
        self.assertIn("sql_check", categories)
        self.assertIn("anomaly", categories)


if __name__ == "__main__":
    unittest.main()
