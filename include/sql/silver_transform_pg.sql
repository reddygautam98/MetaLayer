-- PostgreSQL version - Silver transformations
CREATE TABLE IF NOT EXISTS silver.orders (
  order_id VARCHAR(50) PRIMARY KEY,
  order_date DATE,
  customer_id VARCHAR(50),
  status VARCHAR(30),
  total_amount DECIMAL(18,2)
);

TRUNCATE TABLE silver.orders;

INSERT INTO silver.orders (order_id, order_date, customer_id, status, total_amount)
SELECT
  TRIM(order_id),
  order_date::DATE,
  TRIM(customer_id),
  UPPER(TRIM(status)),
  total_amount
FROM bronze.erp_orders_raw
WHERE order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS silver.customers (
  customer_id VARCHAR(50) PRIMARY KEY,
  customer_name VARCHAR(200),
  email VARCHAR(200),
  phone VARCHAR(50),
  country VARCHAR(100),
  created_at DATE
);

TRUNCATE TABLE silver.customers;

INSERT INTO silver.customers (customer_id, customer_name, email, phone, country, created_at)
SELECT
  TRIM(customer_id),
  NULLIF(TRIM(customer_name), ''),
  LOWER(NULLIF(TRIM(email), '')),
  NULLIF(TRIM(phone), ''),
  UPPER(NULLIF(TRIM(country), '')),
  created_at::DATE
FROM bronze.crm_customers_raw
WHERE customer_id IS NOT NULL;