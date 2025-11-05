IF OBJECT_ID('bronze.erp_orders_raw') IS NULL
CREATE TABLE bronze.erp_orders_raw (
  order_id        NVARCHAR(50),
  order_date      DATETIME2,
  customer_id     NVARCHAR(50),
  status          NVARCHAR(30),
  total_amount    DECIMAL(18,2)
);

IF OBJECT_ID('bronze.crm_customers_raw') IS NULL
CREATE TABLE bronze.crm_customers_raw (
  customer_id     NVARCHAR(50),
  customer_name   NVARCHAR(200),
  email           NVARCHAR(200),
  phone           NVARCHAR(50),
  country         NVARCHAR(100),
  created_at      DATETIME2
);
