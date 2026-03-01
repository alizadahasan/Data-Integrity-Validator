from __future__ import annotations

import argparse
import json
from pathlib import Path

from integrity_validator import DataIntegrityValidator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate data integrity across files and SQLite tables."
    )
    parser.add_argument("--data-dir", default="data", help="Directory containing CSV/JSON files")
    parser.add_argument(
        "--db-path", default="data/warehouse.db", help="Path to SQLite database"
    )
    parser.add_argument(
        "--schema-path",
        default="data/expected_schema.json",
        help="Path to expected schema JSON",
    )
    parser.add_argument(
        "--sql-checks",
        default="sql/consistency_checks.sql",
        help="Path to SQL consistency checks",
    )
    parser.add_argument(
        "--report-path",
        default="reports/integrity_report.json",
        help="Path where JSON report is written",
    )
    parser.add_argument(
        "--fail-on-high",
        action="store_true",
        help="Return exit code 1 when high severity issues are found",
    )
    return parser.parse_args()


def print_summary(report: dict) -> None:
    summary = report["summary"]
    print("Data Integrity Validation Report")
    print("=" * 32)
    print(f"Files scanned:  {summary['files_scanned']}")
    print(f"Total issues:   {summary['total_issues']}")
    print(f"High severity:  {summary['high']}")
    print(f"Medium severity:{summary['medium']}")
    print(f"Low severity:   {summary['low']}")

    issues = report["issues"]
    if issues:
        print("\nTop findings:")
        for issue in issues[:10]:
            print(
                f"- [{issue['severity'].upper()}] {issue['category']} | "
                f"{issue['location']} | {issue['message']}"
            )
    else:
        print("\nNo integrity issues detected.")


def main() -> int:
    args = parse_args()

    validator = DataIntegrityValidator(
        data_dir=args.data_dir,
        db_path=args.db_path,
        schema_path=args.schema_path,
        sql_checks_path=args.sql_checks,
    )
    report = validator.run()

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print_summary(report)
    print(f"\nFull JSON report: {report_path}")

    if args.fail_on_high and report["summary"]["high"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
