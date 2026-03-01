from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

SOURCE_CUSTOMERS = [
    (1, "Alice Johnson", "alice@example.com", "2024-01-10"),
    (2, "Bob Smith", "bob@example.com", "2024-02-03"),
    (3, "Charlie Adams", "charlie@example.com", "2024-03-01"),
]

SOURCE_ORDERS = [
    (1001, 1, 120.50, "2024-05-10"),
    (1002, 2, 50.00, "2024-05-11"),
    (1003, 3, 75.00, "2024-05-13"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build sample SQLite DB for validator demo.")
    parser.add_argument("--db-path", default="data/warehouse.db", help="SQLite DB output path")
    parser.add_argument("--schema-sql", default="sql/schema.sql", help="Schema SQL file path")
    parser.add_argument(
        "--customers-csv",
        default="data/source/customers.csv",
        help="CSV file loaded into backup_customers_csv",
    )
    parser.add_argument(
        "--orders-json",
        default="data/backup/orders_backup.json",
        help="JSON file loaded into backup_orders_json",
    )
    return parser.parse_args()


def load_backup_customers(csv_path: Path) -> list[tuple[int | None, str, str, str]]:
    rows: list[tuple[int | None, str, str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            customer_id_raw = (row.get("customer_id") or "").strip()
            customer_id = int(customer_id_raw) if customer_id_raw.isdigit() else None
            rows.append(
                (
                    customer_id,
                    row.get("name") or "",
                    row.get("email") or "",
                    row.get("signup_date") or "",
                )
            )
    return rows


def load_backup_orders(json_path: Path) -> list[tuple[int | None, int | None, float | None, str]]:
    parsed = json.loads(json_path.read_text(encoding="utf-8"))
    rows: list[tuple[int | None, int | None, float | None, str]] = []

    for item in parsed:
        order_id = item.get("order_id")
        customer_id = item.get("customer_id")
        order_amount = item.get("order_amount")
        order_date = item.get("order_date") or ""

        rows.append(
            (
                int(order_id) if isinstance(order_id, (int, str)) and str(order_id).isdigit() else None,
                int(customer_id)
                if isinstance(customer_id, (int, str)) and str(customer_id).isdigit()
                else None,
                float(order_amount) if isinstance(order_amount, (int, float, str)) else None,
                order_date,
            )
        )

    return rows


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path)
    schema_sql_path = Path(args.schema_sql)
    customers_csv_path = Path(args.customers_csv)
    orders_json_path = Path(args.orders_json)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(schema_sql_path.read_text(encoding="utf-8"))

        for table_name in [
            "customers",
            "orders",
            "backup_customers_csv",
            "backup_orders_json",
        ]:
            connection.execute(f"DELETE FROM {table_name};")

        connection.executemany(
            "INSERT INTO customers(customer_id, name, email, signup_date) VALUES (?, ?, ?, ?)",
            SOURCE_CUSTOMERS,
        )
        connection.executemany(
            "INSERT INTO orders(order_id, customer_id, order_amount, order_date) VALUES (?, ?, ?, ?)",
            SOURCE_ORDERS,
        )

        backup_customers = load_backup_customers(customers_csv_path)
        backup_orders = load_backup_orders(orders_json_path)

        connection.executemany(
            "INSERT INTO backup_customers_csv(customer_id, name, email, signup_date) VALUES (?, ?, ?, ?)",
            backup_customers,
        )
        connection.executemany(
            "INSERT INTO backup_orders_json(order_id, customer_id, order_amount, order_date) VALUES (?, ?, ?, ?)",
            backup_orders,
        )

        connection.commit()
    finally:
        connection.close()

    print(f"Sample database created at: {db_path}")


if __name__ == "__main__":
    main()
