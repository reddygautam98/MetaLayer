-- Gold layer DDL for PostgreSQL
CREATE TABLE IF NOT EXISTS gold.dim_customer (
  customer_key SERIAL PRIMARY KEY,
  customer_id VARCHAR(50) UNIQUE,
  customer_name VARCHAR(200),
  email VARCHAR(200),
  phone VARCHAR(50),
  country VARCHAR(100)
);
TRUNCATE TABLE gold.dim_customer;
INSERT INTO gold.dim_customer (customer_id, customer_name, email, phone, country)
SELECT customer_id, customer_name, email, phone, country
FROM silver.customers;

CREATE TABLE IF NOT EXISTS gold.fact_sales (
  order_id VARCHAR(50) PRIMARY KEY,
  order_date DATE,
  customer_key INT,
  status VARCHAR(30),
  total_amount DECIMAL(18,2),
  CONSTRAINT fk_sales_dim_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customer(customer_key)
);
TRUNCATE TABLE gold.fact_sales;
INSERT INTO gold.fact_sales (order_id, order_date, customer_key, status, total_amount)
SELECT
  s.order_id,
  s.order_date,
  dc.customer_key,
  CASE WHEN s.status IN ('PAID','CLOSED','COMPLETE') THEN 'COMPLETED' ELSE s.status END,
  s.total_amount
FROM silver.orders s
JOIN gold.dim_customer dc ON dc.customer_id = s.customer_id;
