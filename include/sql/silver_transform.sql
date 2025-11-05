IF OBJECT_ID('silver.orders') IS NULL
CREATE TABLE silver.orders (
  order_id NVARCHAR(50) PRIMARY KEY,
  order_date DATE,
  customer_id NVARCHAR(50),
  status NVARCHAR(30),
  total_amount DECIMAL(18,2)
);
TRUNCATE TABLE silver.orders;
INSERT INTO silver.orders (order_id, order_date, customer_id, status, total_amount)
SELECT
  LTRIM(RTRIM(order_id)),
  CAST(order_date AS DATE),
  LTRIM(RTRIM(customer_id)),
  UPPER(LTRIM(RTRIM(status))),
  total_amount
FROM bronze.erp_orders_raw
WHERE order_id IS NOT NULL;

IF OBJECT_ID('silver.customers') IS NULL
CREATE TABLE silver.customers (
  customer_id NVARCHAR(50) PRIMARY KEY,
  customer_name NVARCHAR(200),
  email NVARCHAR(200),
  phone NVARCHAR(50),
  country NVARCHAR(100),
  created_at DATE
);
TRUNCATE TABLE silver.customers;
INSERT INTO silver.customers (customer_id, customer_name, email, phone, country, created_at)
SELECT
  LTRIM(RTRIM(customer_id)),
  NULLIF(LTRIM(RTRIM(customer_name)), ''),
  LOWER(NULLIF(LTRIM(RTRIM(email)), '')),
  NULLIF(LTRIM(RTRIM(phone)), ''),
  UPPER(NULLIF(LTRIM(RTRIM(country)), '')),
  CAST(created_at AS DATE)
FROM bronze.crm_customers_raw
WHERE customer_id IS NOT NULL;
