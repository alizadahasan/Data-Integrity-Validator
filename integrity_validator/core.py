from __future__ import annotations

import csv
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
DATE_FORMAT = "%Y-%m-%d"


@dataclass
class ValidationIssue:
    severity: str
    category: str
    location: str
    message: str
    details: dict[str, Any]


class DataIntegrityValidator:
    def __init__(
        self,
        data_dir: str,
        db_path: str,
        schema_path: str,
        sql_checks_path: str,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.sql_checks_path = Path(sql_checks_path)
        self.schema = self._load_schema()
        self.files_scanned = 0

    def run(self) -> dict[str, Any]:
        issues: list[ValidationIssue] = []
        issues.extend(self._crawl_and_validate_files())
        issues.extend(self._validate_database())

        summary = self._build_summary(issues)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": summary,
            "issues": [asdict(issue) for issue in issues],
        }

    def _load_schema(self) -> dict[str, Any]:
        try:
            return json.loads(self.schema_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"datasets": {}, "database": {}}
        except json.JSONDecodeError as exc:
            return {
                "datasets": {},
                "database": {},
                "schema_error": f"Schema file is invalid JSON: {exc}",
            }

    def _crawl_and_validate_files(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        schema_error = self.schema.get("schema_error")
        if schema_error:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="schema",
                    location=str(self.schema_path),
                    message=schema_error,
                    details={},
                )
            )
            return issues

        if not self.data_dir.exists():
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="filesystem",
                    location=str(self.data_dir),
                    message="Data directory does not exist.",
                    details={},
                )
            )
            return issues

        schema_resolved = self.schema_path.resolve()
        data_files = sorted(
            [
                path
                for path in self.data_dir.rglob("*")
                if path.is_file()
                and path.suffix.lower() in {".csv", ".json"}
                and path.resolve() != schema_resolved
            ]
        )

        if not data_files:
            issues.append(
                ValidationIssue(
                    severity="medium",
                    category="filesystem",
                    location=str(self.data_dir),
                    message="No CSV or JSON files found to validate.",
                    details={},
                )
            )
            return issues

        self.files_scanned = len(data_files)

        for file_path in data_files:
            dataset_name = self._dataset_name_for_file(file_path)
            if not dataset_name:
                issues.append(
                    ValidationIssue(
                        severity="low",
                        category="structure",
                        location=str(file_path),
                        message="No matching dataset schema; file skipped.",
                        details={},
                    )
                )
                continue

            dataset_schema = self.schema.get("datasets", {}).get(dataset_name, {})
            if file_path.suffix.lower() == ".csv":
                issues.extend(self._validate_csv(file_path, dataset_name, dataset_schema))
            elif file_path.suffix.lower() == ".json":
                issues.extend(self._validate_json(file_path, dataset_name, dataset_schema))

        return issues

    def _dataset_name_for_file(self, file_path: Path) -> str | None:
        name = file_path.stem.lower()
        for dataset_name in self.schema.get("datasets", {}):
            if name.startswith(dataset_name.lower()):
                return dataset_name
        return None

    def _validate_csv(
        self, file_path: Path, dataset_name: str, dataset_schema: dict[str, Any]
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        try:
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames is None:
                    issues.append(
                        ValidationIssue(
                            severity="high",
                            category="structure",
                            location=str(file_path),
                            message="CSV appears corrupted (missing headers).",
                            details={"dataset": dataset_name},
                        )
                    )
                    return issues

                required_columns = dataset_schema.get("required_columns", [])
                missing_columns = [
                    column for column in required_columns if column not in reader.fieldnames
                ]
                if missing_columns:
                    issues.append(
                        ValidationIssue(
                            severity="high",
                            category="structure",
                            location=str(file_path),
                            message="CSV is missing required columns.",
                            details={
                                "dataset": dataset_name,
                                "missing_columns": missing_columns,
                            },
                        )
                    )
                    return issues

                records = list(reader)
                issues.extend(
                    self._validate_records(
                        records=records,
                        dataset_name=dataset_name,
                        dataset_schema=dataset_schema,
                        source_label=str(file_path),
                    )
                )
        except UnicodeDecodeError:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="structure",
                    location=str(file_path),
                    message="File encoding issue; unable to decode as UTF-8.",
                    details={"dataset": dataset_name},
                )
            )
        except OSError as exc:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="filesystem",
                    location=str(file_path),
                    message="Failed to read CSV file.",
                    details={"dataset": dataset_name, "error": str(exc)},
                )
            )

        return issues

    def _validate_json(
        self, file_path: Path, dataset_name: str, dataset_schema: dict[str, Any]
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        try:
            raw_data = json.loads(file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="structure",
                    location=str(file_path),
                    message="JSON is corrupted or malformed.",
                    details={"dataset": dataset_name, "error": str(exc)},
                )
            )
            return issues
        except OSError as exc:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="filesystem",
                    location=str(file_path),
                    message="Failed to read JSON file.",
                    details={"dataset": dataset_name, "error": str(exc)},
                )
            )
            return issues

        if not isinstance(raw_data, list):
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="structure",
                    location=str(file_path),
                    message="JSON dataset must be a list of records.",
                    details={"dataset": dataset_name},
                )
            )
            return issues

        records: list[dict[str, Any]] = []
        for index, item in enumerate(raw_data, start=1):
            if not isinstance(item, dict):
                issues.append(
                    ValidationIssue(
                        severity="high",
                        category="structure",
                        location=f"{file_path}#item{index}",
                        message="Record is not a JSON object.",
                        details={"dataset": dataset_name},
                    )
                )
                continue
            records.append(item)

        issues.extend(
            self._validate_records(
                records=records,
                dataset_name=dataset_name,
                dataset_schema=dataset_schema,
                source_label=str(file_path),
            )
        )
        return issues

    def _validate_records(
        self,
        records: list[dict[str, Any]],
        dataset_name: str,
        dataset_schema: dict[str, Any],
        source_label: str,
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        primary_key = dataset_schema.get("primary_key")
        seen_keys: set[str] = set()

        for row_number, record in enumerate(records, start=1):
            location = f"{source_label}:row{row_number}"

            if primary_key:
                pk_value = record.get(primary_key)
                if self._is_empty(pk_value):
                    issues.append(
                        ValidationIssue(
                            severity="high",
                            category="structure",
                            location=location,
                            message="Primary key value is missing.",
                            details={"dataset": dataset_name, "primary_key": primary_key},
                        )
                    )
                else:
                    normalized_key = str(pk_value).strip()
                    if normalized_key in seen_keys:
                        issues.append(
                            ValidationIssue(
                                severity="medium",
                                category="anomaly",
                                location=location,
                                message="Duplicate primary key found.",
                                details={"dataset": dataset_name, "value": normalized_key},
                            )
                        )
                    seen_keys.add(normalized_key)

            for column in dataset_schema.get("required_columns", []):
                if self._is_empty(record.get(column)):
                    issues.append(
                        ValidationIssue(
                            severity="high",
                            category="structure",
                            location=location,
                            message="Required value is missing.",
                            details={"dataset": dataset_name, "column": column},
                        )
                    )

            for column in dataset_schema.get("integer_columns", []):
                value = record.get(column)
                if self._is_empty(value):
                    continue
                try:
                    int(str(value))
                except (TypeError, ValueError):
                    issues.append(
                        ValidationIssue(
                            severity="medium",
                            category="anomaly",
                            location=location,
                            message="Invalid integer value.",
                            details={"dataset": dataset_name, "column": column, "value": value},
                        )
                    )

            for column in dataset_schema.get("numeric_columns", []):
                value = record.get(column)
                if self._is_empty(value):
                    continue
                try:
                    float(str(value))
                except (TypeError, ValueError):
                    issues.append(
                        ValidationIssue(
                            severity="medium",
                            category="anomaly",
                            location=location,
                            message="Invalid numeric value.",
                            details={"dataset": dataset_name, "column": column, "value": value},
                        )
                    )

            for column in dataset_schema.get("positive_numeric_columns", []):
                value = record.get(column)
                if self._is_empty(value):
                    continue
                try:
                    numeric_value = float(str(value))
                    if numeric_value <= 0:
                        issues.append(
                            ValidationIssue(
                                severity="medium",
                                category="anomaly",
                                location=location,
                                message="Numeric value should be positive.",
                                details={
                                    "dataset": dataset_name,
                                    "column": column,
                                    "value": value,
                                },
                            )
                        )
                except (TypeError, ValueError):
                    continue

            for column in dataset_schema.get("email_columns", []):
                value = record.get(column)
                if self._is_empty(value):
                    continue
                if not EMAIL_REGEX.match(str(value).strip()):
                    issues.append(
                        ValidationIssue(
                            severity="medium",
                            category="anomaly",
                            location=location,
                            message="Invalid email format.",
                            details={"dataset": dataset_name, "column": column, "value": value},
                        )
                    )

            for column in dataset_schema.get("date_columns", []):
                value = record.get(column)
                if self._is_empty(value):
                    continue
                try:
                    datetime.strptime(str(value), DATE_FORMAT)
                except ValueError:
                    issues.append(
                        ValidationIssue(
                            severity="medium",
                            category="anomaly",
                            location=location,
                            message="Invalid date format. Expected YYYY-MM-DD.",
                            details={"dataset": dataset_name, "column": column, "value": value},
                        )
                    )

        return issues

    def _validate_database(self) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        if not self.db_path.exists():
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="database",
                    location=str(self.db_path),
                    message="Database file does not exist. Run setup script first.",
                    details={},
                )
            )
            return issues

        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row

        try:
            integrity_row = connection.execute("PRAGMA integrity_check;").fetchone()
            integrity_status = str(integrity_row[0]) if integrity_row else "unknown"
            if integrity_status.lower() != "ok":
                issues.append(
                    ValidationIssue(
                        severity="high",
                        category="database",
                        location=str(self.db_path),
                        message="SQLite integrity check failed.",
                        details={"status": integrity_status},
                    )
                )

            issues.extend(self._validate_required_tables(connection))
            issues.extend(self._run_source_table_anomaly_queries(connection))
            issues.extend(self._run_sql_consistency_views(connection))
        except sqlite3.DatabaseError as exc:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="database",
                    location=str(self.db_path),
                    message="Database validation failed due to SQL error.",
                    details={"error": str(exc)},
                )
            )
        finally:
            connection.close()

        return issues

    def _validate_required_tables(
        self, connection: sqlite3.Connection
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        configured_tables = set(
            self.schema.get("database", {}).get("required_tables", [])
        )
        existing_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        }
        missing_tables = sorted(configured_tables - existing_tables)

        for table in missing_tables:
            issues.append(
                ValidationIssue(
                    severity="high",
                    category="database",
                    location=table,
                    message="Required table is missing.",
                    details={},
                )
            )

        return issues

    def _run_source_table_anomaly_queries(
        self, connection: sqlite3.Connection
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        checks = [
            {
                "name": "orphan_source_orders",
                "sql": (
                    "SELECT order_id, customer_id "
                    "FROM orders "
                    "WHERE customer_id NOT IN (SELECT customer_id FROM customers)"
                ),
                "message": "Order references a missing customer in source tables.",
            },
            {
                "name": "non_positive_source_order_amount",
                "sql": "SELECT order_id, order_amount FROM orders WHERE order_amount <= 0",
                "message": "Source order has non-positive amount.",
            },
        ]

        for check in checks:
            try:
                rows = connection.execute(check["sql"]).fetchall()
            except sqlite3.DatabaseError as exc:
                issues.append(
                    ValidationIssue(
                        severity="high",
                        category="database",
                        location=check["name"],
                        message="Failed to run source anomaly query.",
                        details={"error": str(exc)},
                    )
                )
                continue

            for row in rows:
                issues.append(
                    ValidationIssue(
                        severity="medium",
                        category="sql_check",
                        location=check["name"],
                        message=check["message"],
                        details=dict(row),
                    )
                )

        return issues

    def _run_sql_consistency_views(
        self, connection: sqlite3.Connection
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        if not self.sql_checks_path.exists():
            issues.append(
                ValidationIssue(
                    severity="medium",
                    category="sql_check",
                    location=str(self.sql_checks_path),
                    message="SQL consistency check file not found.",
                    details={},
                )
            )
            return issues

        sql_script = self.sql_checks_path.read_text(encoding="utf-8")
        connection.executescript(sql_script)

        view_names: list[str] = self.schema.get("database", {}).get(
            "consistency_views", []
        )
        for view_name in view_names:
            try:
                rows = connection.execute(f"SELECT * FROM {view_name};").fetchall()
            except sqlite3.DatabaseError as exc:
                issues.append(
                    ValidationIssue(
                        severity="high",
                        category="sql_check",
                        location=view_name,
                        message="Failed to run SQL consistency view.",
                        details={"error": str(exc)},
                    )
                )
                continue

            for row in rows:
                issues.append(
                    ValidationIssue(
                        severity="medium",
                        category="sql_check",
                        location=view_name,
                        message="Cross-format consistency mismatch detected.",
                        details=dict(row),
                    )
                )

        return issues

    def _build_summary(self, issues: list[ValidationIssue]) -> dict[str, Any]:
        counts = {"low": 0, "medium": 0, "high": 0}
        for issue in issues:
            if issue.severity in counts:
                counts[issue.severity] += 1

        return {
            "files_scanned": self.files_scanned,
            "total_issues": len(issues),
            "high": counts["high"],
            "medium": counts["medium"],
            "low": counts["low"],
        }

    @staticmethod
    def _is_empty(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        return False
