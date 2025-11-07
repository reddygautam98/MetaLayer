-- Bronze layer DDL for PostgreSQL
CREATE TABLE IF NOT EXISTS bronze.erp_orders_raw (
  order_id        VARCHAR(50),
  order_date      TIMESTAMP,
  customer_id     VARCHAR(50),
  status          VARCHAR(30),
  total_amount    DECIMAL(18,2)
);

CREATE TABLE IF NOT EXISTS bronze.crm_customers_raw (
  customer_id     VARCHAR(50),
  customer_name   VARCHAR(200),
  email           VARCHAR(200),
  phone           VARCHAR(50),
  country         VARCHAR(100),
  created_at      TIMESTAMP
);
