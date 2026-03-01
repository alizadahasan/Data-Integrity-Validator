# Data Integrity Validator (Python + SQL)

A junior-level portfolio project that validates data integrity across CSV, JSON, and SQLite.

## What this project does

- Crawls a data folder and validates CSV/JSON structure.
- Detects corruption/anomalies such as:
  - missing required fields
  - duplicate primary keys
  - invalid emails/dates
  - non-positive numeric values
- Runs SQL integrity checks in SQLite.
- Runs cross-format SQL consistency checks between:
  - source tables (`customers`, `orders`)
  - backup/staging tables loaded from CSV and JSON
- Produces a JSON report for backup verification evidence.

## Project structure

```text
.
├── main.py
├── integrity_validator/
│   ├── __init__.py
│   └── core.py
├── scripts/
│   └── setup_sample_db.py
├── sql/
│   ├── schema.sql
│   └── consistency_checks.sql
├── data/
│   ├── expected_schema.json
│   ├── source/
│   │   ├── customers.csv
│   │   └── orders.csv
│   └── backup/
│       ├── customers_backup.json
│       └── orders_backup.json
├── tests/
│   └── test_validator.py
└── reports/
```

## Quick start

1. Create the sample SQLite database:

```bash
python3 scripts/setup_sample_db.py
```

2. Run the validator:

```bash
python3 main.py
```

3. Optional: fail CI when high severity issues exist:

```bash
python3 main.py --fail-on-high
```

4. Run tests:

```bash
python3 -m unittest discover -s tests -v
```

## Example output

```text
Data Integrity Validation Report
================================
Files scanned:  4
Total issues:   17
High severity:  1
Medium severity:15
Low severity:   1
```

A full report is saved to:

- `reports/integrity_report.json`

## SQL consistency checks included

The file `sql/consistency_checks.sql` creates views for checks such as:

- source vs backup record count mismatches
- missing records in backup
- extra records in backup
- order amount mismatches by key

This directly maps to data resilience and backup verification workflows.

## Ideas to extend

- Add checksum validation (MD5/SHA256) per dataset snapshot.
- Add scheduled execution via cron or GitHub Actions.
- Add Slack/email alerting for high severity issues.
- Add support for PostgreSQL/MySQL sources.

## Resume bullet alignment

- Built a Python/SQL Data Integrity Validator to crawl CSV/JSON/SQLite data and detect corruption/anomalies.
- Wrote SQL consistency checks to compare source and backup datasets across different storage formats.
- Produced machine-readable integrity reports to support backup verification and resilience tasks.
