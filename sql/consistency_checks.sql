DROP VIEW IF EXISTS check_customer_count_mismatch;
CREATE VIEW check_customer_count_mismatch AS
WITH source_count AS (
    SELECT COUNT(*) AS count_value FROM customers
),
backup_count AS (
    SELECT COUNT(*) AS count_value FROM backup_customers_csv
)
SELECT source_count.count_value AS source_count,
       backup_count.count_value AS backup_count
FROM source_count, backup_count
WHERE source_count.count_value <> backup_count.count_value;

DROP VIEW IF EXISTS check_missing_customers_in_backup_csv;
CREATE VIEW check_missing_customers_in_backup_csv AS
SELECT c.customer_id
FROM customers c
LEFT JOIN backup_customers_csv b ON b.customer_id = c.customer_id
WHERE b.customer_id IS NULL;

DROP VIEW IF EXISTS check_extra_customers_in_backup_csv;
CREATE VIEW check_extra_customers_in_backup_csv AS
SELECT b.customer_id
FROM backup_customers_csv b
LEFT JOIN customers c ON c.customer_id = b.customer_id
WHERE c.customer_id IS NULL;

DROP VIEW IF EXISTS check_order_count_mismatch;
CREATE VIEW check_order_count_mismatch AS
WITH source_count AS (
    SELECT COUNT(*) AS count_value FROM orders
),
backup_count AS (
    SELECT COUNT(*) AS count_value FROM backup_orders_json
)
SELECT source_count.count_value AS source_count,
       backup_count.count_value AS backup_count
FROM source_count, backup_count
WHERE source_count.count_value <> backup_count.count_value;

DROP VIEW IF EXISTS check_missing_orders_in_backup_json;
CREATE VIEW check_missing_orders_in_backup_json AS
SELECT o.order_id
FROM orders o
LEFT JOIN backup_orders_json b ON b.order_id = o.order_id
WHERE b.order_id IS NULL;

DROP VIEW IF EXISTS check_extra_orders_in_backup_json;
CREATE VIEW check_extra_orders_in_backup_json AS
SELECT b.order_id
FROM backup_orders_json b
LEFT JOIN orders o ON o.order_id = b.order_id
WHERE o.order_id IS NULL;

DROP VIEW IF EXISTS check_order_amount_mismatch;
CREATE VIEW check_order_amount_mismatch AS
SELECT o.order_id,
       o.order_amount AS source_amount,
       b.order_amount AS backup_amount
FROM orders o
INNER JOIN backup_orders_json b ON b.order_id = o.order_id
WHERE ABS(o.order_amount - b.order_amount) > 0.0001;
