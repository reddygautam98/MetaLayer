-- PostgreSQL version - Bronze DDL (Production Schema)
-- Matches the schema used in bronze_layer_production.py

-- ERP Sales Bronze Table (Production Schema)
CREATE TABLE IF NOT EXISTS bronze.erp_sales_raw (
    sales_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity INTEGER CHECK (quantity > 0),
    sale_date DATE NOT NULL,
    sale_amount DECIMAL(12,2) CHECK (sale_amount > 0),
    loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_erp_sales_raw PRIMARY KEY (sales_id)
);

-- CRM Customers Bronze Table (Production Schema)  
CREATE TABLE IF NOT EXISTS bronze.crm_customers_raw (
    customer_id INTEGER NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(200) CHECK (email LIKE '%@%'),
    region VARCHAR(50) NOT NULL,
    loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_crm_customers_raw PRIMARY KEY (customer_id)
);

-- Create performance indexes
CREATE INDEX IF NOT EXISTS idx_erp_sales_customer_id ON bronze.erp_sales_raw(customer_id);
CREATE INDEX IF NOT EXISTS idx_erp_sales_product_id ON bronze.erp_sales_raw(product_id);
CREATE INDEX IF NOT EXISTS idx_erp_sales_date ON bronze.erp_sales_raw(sale_date);
CREATE INDEX IF NOT EXISTS idx_crm_customers_region ON bronze.crm_customers_raw(region);
CREATE INDEX IF NOT EXISTS idx_crm_customers_email ON bronze.crm_customers_raw(email);