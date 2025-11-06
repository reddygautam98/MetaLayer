#!/bin/bash
# =====================================================
# DATABASE INITIALIZATION SCRIPT FOR ETL PIPELINE
# Sets up PostgreSQL schemas, tables, and initial data
# =====================================================

set -e

echo "🚀 Starting ETL Pipeline Database Initialization..."

# Wait for PostgreSQL to be ready
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 2
done

echo "✅ PostgreSQL is ready!"

# Create ETL schemas
echo "📊 Creating ETL schemas..."
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" << 'EOF'
-- Create ETL schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;  
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS airflow_meta;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA airflow_meta TO postgres;

-- Create ETL metadata tables
CREATE TABLE IF NOT EXISTS airflow_meta.etl_run_log (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50) DEFAULT 'RUNNING',
    records_processed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create data quality tracking table
CREATE TABLE IF NOT EXISTS airflow_meta.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    check_type VARCHAR(100) NOT NULL,
    check_result BOOLEAN NOT NULL,
    check_value NUMERIC,
    threshold_value NUMERIC,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dag_run_id VARCHAR(255),
    error_details TEXT
);

-- Create performance monitoring table
CREATE TABLE IF NOT EXISTS airflow_meta.pipeline_performance (
    perf_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    total_duration_seconds INTEGER,
    records_per_second NUMERIC,
    memory_usage_mb INTEGER,
    cpu_usage_percent NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_etl_run_log_dag_id ON airflow_meta.etl_run_log(dag_id);
CREATE INDEX IF NOT EXISTS idx_etl_run_log_execution_date ON airflow_meta.etl_run_log(execution_date);
CREATE INDEX IF NOT EXISTS idx_data_quality_table_name ON airflow_meta.data_quality_checks(table_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_performance_dag_id ON airflow_meta.pipeline_performance(dag_id);

EOF

echo "✅ ETL schemas and metadata tables created successfully!"

# Insert initial configuration data
echo "⚙️ Inserting initial configuration..."
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" << 'EOF'
-- Create configuration table
CREATE TABLE IF NOT EXISTS airflow_meta.etl_config (
    config_key VARCHAR(255) PRIMARY KEY,
    config_value TEXT NOT NULL,
    description TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert default configuration
INSERT INTO airflow_meta.etl_config (config_key, config_value, description) VALUES
('bronze.batch_size', '10000', 'Default batch size for bronze layer processing'),
('silver.batch_size', '5000', 'Default batch size for silver layer processing'),
('data_quality.enabled', 'true', 'Enable data quality checks'),
('data_quality.threshold', '0.05', 'Acceptable error rate threshold'),
('monitoring.enabled', 'true', 'Enable ETL monitoring'),
('retention.bronze_days', '30', 'Retention period for bronze layer data'),
('retention.silver_days', '90', 'Retention period for silver layer data'),
('retention.gold_days', '365', 'Retention period for gold layer data')
ON CONFLICT (config_key) DO NOTHING;

EOF

echo "✅ Initial configuration inserted!"

# Create sample tables for development
echo "🏗️ Creating sample tables for development..."
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" << 'EOF'
-- Bronze layer sample tables
CREATE TABLE IF NOT EXISTS bronze.customers_raw (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100),
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    created_date VARCHAR(50),
    source_system VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    order_date VARCHAR(50),
    quantity VARCHAR(50),
    price VARCHAR(50),
    total_amount VARCHAR(50),
    source_system VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver layer sample tables
CREATE TABLE IF NOT EXISTS silver.customers_cleaned (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    created_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    data_quality_score NUMERIC(3,2),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.orders_cleaned (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    is_valid BOOLEAN DEFAULT TRUE,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold layer sample tables  
CREATE TABLE IF NOT EXISTS gold.customer_analytics (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(255),
    total_orders INTEGER,
    total_amount_spent NUMERIC(12,2),
    average_order_value NUMERIC(10,2),
    first_order_date DATE,
    last_order_date DATE,
    customer_lifetime_value NUMERIC(12,2),
    customer_segment VARCHAR(50),
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gold.sales_summary (
    summary_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue NUMERIC(15,2),
    unique_customers INTEGER,
    average_order_value NUMERIC(10,2),
    top_product_id INTEGER,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

EOF

echo "✅ Sample tables created successfully!"

echo "🎉 ETL Pipeline Database Initialization Complete!"
echo "📊 Schemas created: bronze, silver, gold, airflow_meta"
echo "🗄️ Metadata tables: etl_run_log, data_quality_checks, pipeline_performance"
echo "⚙️ Configuration table: etl_config"
echo "🏗️ Sample tables: customers, orders, analytics"