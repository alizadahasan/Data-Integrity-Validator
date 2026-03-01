CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    signup_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_amount REAL NOT NULL,
    order_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backup_customers_csv (
    customer_id INTEGER,
    name TEXT,
    email TEXT,
    signup_date TEXT
);

CREATE TABLE IF NOT EXISTS backup_orders_json (
    order_id INTEGER,
    customer_id INTEGER,
    order_amount REAL,
    order_date TEXT
);
