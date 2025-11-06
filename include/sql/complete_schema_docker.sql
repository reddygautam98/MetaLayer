/*
COMPLETE DATABASE SCHEMA INITIALIZATION FOR METALAYER ETL PIPELINE
================================================================

This script creates all necessary schemas, tables, and supporting structures
for the Bronze -> Silver -> Gold medallion architecture ETL pipeline.

Schemas Created:
- bronze: Raw data ingestion layer
- silver: Cleaned and transformed data layer  
- gold: Analytics and dimensional modeling layer
- airflow_meta: Pipeline metadata and monitoring

Features:
- Docker PostgreSQL optimized configurations
- Comprehensive indexing strategy
- Partitioning for large tables
- Data quality and audit columns
- Performance monitoring tables
*/

-- =====================================================
-- CREATE SCHEMAS
-- =====================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS airflow_meta;

-- Set search path for easy access
SET search_path TO bronze, silver, gold, airflow_meta, public;

-- =====================================================
-- BRONZE LAYER TABLES (Raw Data Ingestion)
-- =====================================================

-- Bronze Customer Raw Data
DROP TABLE IF EXISTS bronze.customers_raw CASCADE;
CREATE TABLE bronze.customers_raw (
    -- Primary identifiers
    record_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    
    -- Customer data fields
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    
    -- Source system fields
    source_system VARCHAR(50) NOT NULL,
    source_file_path VARCHAR(500),
    source_row_number INTEGER,
    
    -- Data quality and validation
    validation_status VARCHAR(20) DEFAULT 'PENDING',
    validation_errors TEXT,
    data_hash VARCHAR(64),
    
    -- Timestamps
    created_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_timestamp TIMESTAMP WITH TIME ZONE
);

-- Indexes for Bronze customers
CREATE INDEX idx_bronze_customers_customer_id ON bronze.customers_raw(customer_id);
CREATE INDEX idx_bronze_customers_ingestion_date ON bronze.customers_raw(DATE(ingestion_timestamp));
CREATE INDEX idx_bronze_customers_source_system ON bronze.customers_raw(source_system);
CREATE INDEX idx_bronze_customers_validation_status ON bronze.customers_raw(validation_status);

-- Bronze Orders Raw Data
DROP TABLE IF EXISTS bronze.orders_raw CASCADE;
CREATE TABLE bronze.orders_raw (
    -- Primary identifiers
    record_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    
    -- Order data fields
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    order_date DATE,
    quantity INTEGER,
    price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    
    -- Source system fields
    source_system VARCHAR(50) NOT NULL,
    source_file_path VARCHAR(500),
    source_row_number INTEGER,
    
    -- Data quality and validation
    validation_status VARCHAR(20) DEFAULT 'PENDING',
    validation_errors TEXT,
    data_hash VARCHAR(64),
    
    -- Timestamps
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_timestamp TIMESTAMP WITH TIME ZONE
);

-- Indexes for Bronze orders
CREATE INDEX idx_bronze_orders_order_id ON bronze.orders_raw(order_id);
CREATE INDEX idx_bronze_orders_customer_id ON bronze.orders_raw(customer_id);
CREATE INDEX idx_bronze_orders_order_date ON bronze.orders_raw(order_date);
CREATE INDEX idx_bronze_orders_ingestion_date ON bronze.orders_raw(DATE(ingestion_timestamp));
CREATE INDEX idx_bronze_orders_source_system ON bronze.orders_raw(source_system);
CREATE INDEX idx_bronze_orders_validation_status ON bronze.orders_raw(validation_status);

-- Bronze Data Lineage Tracking
DROP TABLE IF EXISTS bronze.data_lineage CASCADE;
CREATE TABLE bronze.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    source_record_id VARCHAR(100),
    target_record_id VARCHAR(100),
    transformation_type VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =====================================================
-- SILVER LAYER TABLES (Cleaned and Transformed Data)
-- =====================================================

-- Silver Customer Cleaned Data
DROP TABLE IF EXISTS silver.customers_cleaned CASCADE;
CREATE TABLE silver.customers_cleaned (
    -- Primary identifiers
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Cleaned customer data
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    
    -- Data quality metrics
    is_active BOOLEAN DEFAULT true,
    data_quality_score DECIMAL(3,2) CHECK (data_quality_score >= 0 AND data_quality_score <= 1),
    
    -- Timestamps
    created_date DATE,
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for Silver customers
CREATE INDEX idx_silver_customers_customer_id ON silver.customers_cleaned(customer_id);
CREATE INDEX idx_silver_customers_active ON silver.customers_cleaned(is_active);
CREATE INDEX idx_silver_customers_quality_score ON silver.customers_cleaned(data_quality_score);
CREATE INDEX idx_silver_customers_processed_date ON silver.customers_cleaned(DATE(processed_timestamp));

-- Silver Orders Cleaned Data
DROP TABLE IF EXISTS silver.orders_cleaned CASCADE;
CREATE TABLE silver.orders_cleaned (
    -- Primary identifiers
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Foreign keys
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    
    -- Cleaned order data
    order_date DATE NOT NULL,
    quantity INTEGER CHECK (quantity > 0),
    unit_price DECIMAL(10,2) CHECK (unit_price >= 0),
    total_amount DECIMAL(12,2) CHECK (total_amount >= 0),
    
    -- Data quality metrics
    is_valid BOOLEAN DEFAULT true,
    
    -- Timestamps
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for Silver orders
CREATE INDEX idx_silver_orders_order_id ON silver.orders_cleaned(order_id);
CREATE INDEX idx_silver_orders_customer_id ON silver.orders_cleaned(customer_id);
CREATE INDEX idx_silver_orders_order_date ON silver.orders_cleaned(order_date);
CREATE INDEX idx_silver_orders_valid ON silver.orders_cleaned(is_valid);
CREATE INDEX idx_silver_orders_processed_date ON silver.orders_cleaned(DATE(processed_timestamp));

-- Silver Data Quality Summary
DROP TABLE IF EXISTS silver.data_quality_summary CASCADE;
CREATE TABLE silver.data_quality_summary (
    summary_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    total_records INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    quality_score DECIMAL(5,4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(table_name, execution_date)
);

-- =====================================================
-- GOLD LAYER TABLES (Analytics and Dimensional Modeling)
-- =====================================================

-- Gold Customer Dimension (SCD Type 2)
DROP TABLE IF EXISTS gold.dim_customer CASCADE;
CREATE TABLE gold.dim_customer (
    -- Surrogate key
    customer_key SERIAL PRIMARY KEY,
    
    -- Natural key
    customer_id VARCHAR(50) NOT NULL,
    
    -- Dimension attributes
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    is_active BOOLEAN DEFAULT true,
    data_quality_score DECIMAL(3,2),
    
    -- SCD Type 2 management
    effective_from TIMESTAMP WITH TIME ZONE NOT NULL,
    effective_to TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '9999-12-31'::timestamp,
    is_current BOOLEAN DEFAULT true,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for Customer Dimension
CREATE INDEX idx_dim_customer_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_current ON gold.dim_customer(is_current);
CREATE INDEX idx_dim_customer_effective_dates ON gold.dim_customer(effective_from, effective_to);
CREATE UNIQUE INDEX idx_dim_customer_current_unique ON gold.dim_customer(customer_id) WHERE is_current = true;

-- Gold Date Dimension
DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_quarter INTEGER,
    day_of_year INTEGER,
    week_of_month INTEGER,
    week_of_year INTEGER,
    month_actual INTEGER,
    month_name VARCHAR(10),
    quarter_actual INTEGER,
    quarter_name VARCHAR(2),
    year_actual INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT false
);

-- Populate Date Dimension with sample data
INSERT INTO gold.dim_date (date_key, date_actual, day_of_week, day_name, day_of_month, 
                          day_of_quarter, day_of_year, week_of_month, week_of_year,
                          month_actual, month_name, quarter_actual, quarter_name, 
                          year_actual, is_weekend)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series as date_actual,
    EXTRACT(DOW FROM date_series) as day_of_week,
    TO_CHAR(date_series, 'Day') as day_name,
    EXTRACT(DAY FROM date_series) as day_of_month,
    EXTRACT(DOY FROM date_series) - EXTRACT(DOY FROM DATE_TRUNC('quarter', date_series)) + 1 as day_of_quarter,
    EXTRACT(DOY FROM date_series) as day_of_year,
    CEIL(EXTRACT(DAY FROM date_series) / 7.0) as week_of_month,
    EXTRACT(WEEK FROM date_series) as week_of_year,
    EXTRACT(MONTH FROM date_series) as month_actual,
    TO_CHAR(date_series, 'Month') as month_name,
    EXTRACT(QUARTER FROM date_series) as quarter_actual,
    'Q' || EXTRACT(QUARTER FROM date_series) as quarter_name,
    EXTRACT(YEAR FROM date_series) as year_actual,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN true ELSE false END as is_weekend
FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) AS date_series
ON CONFLICT (date_key) DO NOTHING;

-- Gold Order Facts Table
DROP TABLE IF EXISTS gold.fact_orders CASCADE;
CREATE TABLE gold.fact_orders (
    -- Surrogate keys
    order_fact_key SERIAL PRIMARY KEY,
    order_date_key INTEGER NOT NULL,
    customer_key INTEGER,
    
    -- Natural key
    order_id VARCHAR(50) NOT NULL UNIQUE,
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    is_valid BOOLEAN DEFAULT true,
    
    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Foreign key constraints
    CONSTRAINT fk_fact_orders_date FOREIGN KEY (order_date_key) REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_orders_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customer(customer_key)
);

-- Indexes for Order Facts
CREATE INDEX idx_fact_orders_date_key ON gold.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_customer_key ON gold.fact_orders(customer_key);
CREATE INDEX idx_fact_orders_order_id ON gold.fact_orders(order_id);
CREATE INDEX idx_fact_orders_valid ON gold.fact_orders(is_valid);

-- Gold Daily Sales Aggregations
DROP TABLE IF EXISTS gold.agg_daily_sales CASCADE;
CREATE TABLE gold.agg_daily_sales (
    date_key INTEGER PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15,2) NOT NULL DEFAULT 0,
    avg_order_value DECIMAL(10,2) NOT NULL DEFAULT 0,
    unique_customers INTEGER NOT NULL DEFAULT 0,
    total_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_agg_daily_sales_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);

-- Gold Monthly Sales Aggregations
DROP TABLE IF EXISTS gold.agg_monthly_sales CASCADE;
CREATE TABLE gold.agg_monthly_sales (
    year_month INTEGER PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(15,2) NOT NULL DEFAULT 0,
    avg_order_value DECIMAL(10,2) NOT NULL DEFAULT 0,
    unique_customers INTEGER NOT NULL DEFAULT 0,
    total_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Gold Business KPIs Table
DROP TABLE IF EXISTS gold.business_kpis CASCADE;
CREATE TABLE gold.business_kpis (
    kpi_id SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL,
    kpi_name VARCHAR(100) NOT NULL,
    kpi_value DECIMAL(15,4) NOT NULL,
    kpi_metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(date_key, kpi_name),
    CONSTRAINT fk_business_kpis_date FOREIGN KEY (date_key) REFERENCES gold.dim_date(date_key)
);

-- =====================================================
-- AIRFLOW METADATA TABLES (Pipeline Monitoring)
-- =====================================================

-- Pipeline Run Tracking
DROP TABLE IF EXISTS airflow_meta.pipeline_runs CASCADE;
CREATE TABLE airflow_meta.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    pipeline_version VARCHAR(20),
    status VARCHAR(20) NOT NULL,
    metadata JSONB,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    total_duration_seconds INTEGER,
    total_records INTEGER,
    quality_score DECIMAL(5,4),
    sla_met BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_time TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(dag_id, execution_date)
);

-- Indexes for Pipeline Runs
CREATE INDEX idx_pipeline_runs_dag_id ON airflow_meta.pipeline_runs(dag_id);
CREATE INDEX idx_pipeline_runs_execution_date ON airflow_meta.pipeline_runs(execution_date);
CREATE INDEX idx_pipeline_runs_status ON airflow_meta.pipeline_runs(status);

-- Pipeline Performance Metrics
DROP TABLE IF EXISTS airflow_meta.pipeline_performance CASCADE;
CREATE TABLE airflow_meta.pipeline_performance (
    metric_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    total_duration_seconds INTEGER NOT NULL,
    records_per_second DECIMAL(10,2),
    memory_usage_mb INTEGER,
    cpu_usage_percent DECIMAL(5,2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Analytics Summary Table
DROP TABLE IF EXISTS airflow_meta.analytics_summary CASCADE;
CREATE TABLE airflow_meta.analytics_summary (
    summary_id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(15,2) DEFAULT 0,
    active_customers INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data Quality Audit Log
DROP TABLE IF EXISTS airflow_meta.data_quality_audit CASCADE;
CREATE TABLE airflow_meta.data_quality_audit (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL,
    check_value DECIMAL(15,4),
    check_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =====================================================
-- PERFORMANCE OPTIMIZATION
-- =====================================================

-- Partitioning for large fact tables (example for future scaling)
-- Note: PostgreSQL native partitioning for orders fact table by date
-- This would be implemented when data volume requires it

-- Table statistics update
ANALYZE bronze.customers_raw;
ANALYZE bronze.orders_raw;
ANALYZE silver.customers_cleaned;
ANALYZE silver.orders_cleaned;
ANALYZE gold.dim_customer;
ANALYZE gold.fact_orders;
ANALYZE gold.agg_daily_sales;
ANALYZE airflow_meta.pipeline_runs;

-- =====================================================
-- SAMPLE DATA INSERTION
-- =====================================================

-- Insert sample customers into Bronze layer
INSERT INTO bronze.customers_raw (customer_id, customer_name, email, phone, address, source_system, validation_status, created_date) VALUES
('CUST001', 'John Smith', 'john.smith@email.com', '555-0101', '123 Main St, City, State', 'CRM_SYSTEM', 'VALID', '2024-01-01'),
('CUST002', 'Jane Doe', 'jane.doe@email.com', '555-0102', '456 Oak Ave, City, State', 'CRM_SYSTEM', 'VALID', '2024-01-01'),
('CUST003', 'Bob Johnson', 'bob.johnson@email.com', '555-0103', '789 Pine St, City, State', 'ERP_SYSTEM', 'VALID', '2024-01-01'),
('CUST004', 'Alice Brown', 'alice.brown@email.com', '555-0104', '321 Elm Dr, City, State', 'CRM_SYSTEM', 'VALID', '2024-01-01'),
('CUST005', 'Charlie Wilson', 'charlie.wilson@email.com', '555-0105', '654 Maple Ln, City, State', 'ERP_SYSTEM', 'VALID', '2024-01-01');

-- Insert sample orders into Bronze layer
INSERT INTO bronze.orders_raw (order_id, customer_id, product_id, order_date, quantity, price, total_amount, source_system, validation_status) VALUES
('ORD001', 'CUST001', 'PROD001', '2024-01-15', 2, 25.99, 51.98, 'ERP_SYSTEM', 'VALID'),
('ORD002', 'CUST002', 'PROD002', '2024-01-15', 1, 149.99, 149.99, 'ERP_SYSTEM', 'VALID'),
('ORD003', 'CUST001', 'PROD003', '2024-01-16', 3, 9.99, 29.97, 'ERP_SYSTEM', 'VALID'),
('ORD004', 'CUST003', 'PROD001', '2024-01-16', 1, 25.99, 25.99, 'ERP_SYSTEM', 'VALID'),
('ORD005', 'CUST004', 'PROD004', '2024-01-17', 2, 75.50, 151.00, 'ERP_SYSTEM', 'VALID'),
('ORD006', 'CUST005', 'PROD002', '2024-01-17', 1, 149.99, 149.99, 'ERP_SYSTEM', 'VALID'),
('ORD007', 'CUST002', 'PROD005', '2024-01-18', 4, 12.25, 49.00, 'ERP_SYSTEM', 'VALID'),
('ORD008', 'CUST001', 'PROD001', '2024-01-18', 1, 25.99, 25.99, 'ERP_SYSTEM', 'VALID');

-- Grant appropriate permissions
GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE ON SCHEMA silver TO PUBLIC;
GRANT USAGE ON SCHEMA gold TO PUBLIC;
GRANT USAGE ON SCHEMA airflow_meta TO PUBLIC;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bronze TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gold TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA airflow_meta TO PUBLIC;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold TO PUBLIC;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA airflow_meta TO PUBLIC;

-- =====================================================
-- COMPLETION MESSAGE
-- =====================================================

SELECT 'MetaLayer ETL Database Schema Creation Completed Successfully!' AS status,
       'Schemas: bronze, silver, gold, airflow_meta' AS schemas_created,
       'Sample data inserted for testing' AS sample_data,
       NOW() AS completed_at;