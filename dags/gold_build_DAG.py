from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='gold_build_DAG',
    default_args=default_args,
    description='Gold Layer Analytics - Build Fact & Dimension Tables for BI Tools',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'analytics', 'facts', 'dimensions', 'bi']
):

    create_gold_tables = SQLExecuteQueryOperator(
        task_id='create_gold_tables',
        conn_id='postgres_default',
        sql='''
        -- Customer Dimension Table
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id VARCHAR(50) UNIQUE,
            customer_name VARCHAR(200),
            email_clean VARCHAR(200),
            phone_clean VARCHAR(50),
            city VARCHAR(100),
            state VARCHAR(50),
            zip VARCHAR(20),
            age_years INTEGER,
            created_date DATE
        );

        -- Product Dimension Table
        CREATE TABLE IF NOT EXISTS gold.dim_product (
            product_key SERIAL PRIMARY KEY,
            product_code VARCHAR(50) UNIQUE,
            product_category VARCHAR(100),
            avg_price DECIMAL(10,2)
        );

        -- Date Dimension Table
        CREATE TABLE IF NOT EXISTS gold.dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            quarter INTEGER,
            day_of_week INTEGER,
            month_name VARCHAR(20)
        );

        -- Sales Fact Table
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            sale_key SERIAL PRIMARY KEY,
            order_id VARCHAR(50),
            customer_key INTEGER,
            product_key INTEGER,
            order_date_key INTEGER,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            total_amount DECIMAL(10,2),
            FOREIGN KEY (customer_key) REFERENCES gold.dim_customer(customer_key),
            FOREIGN KEY (product_key) REFERENCES gold.dim_product(product_key),
            FOREIGN KEY (order_date_key) REFERENCES gold.dim_date(date_key)
        );

        -- Customer Analytics Summary Table
        CREATE TABLE IF NOT EXISTS gold.customer_analytics (
            customer_key INTEGER,
            total_orders INTEGER,
            total_spent DECIMAL(10,2),
            avg_order_value DECIMAL(10,2),
            first_order_date DATE,
            last_order_date DATE,
            customer_lifetime_days INTEGER,
            FOREIGN KEY (customer_key) REFERENCES gold.dim_customer(customer_key)
        );
        '''
    )

    load_customer_dimension = SQLExecuteQueryOperator(
        task_id='load_customer_dimension',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE gold.dim_customer CASCADE;
        
        INSERT INTO gold.dim_customer (
            customer_id, customer_name, email_clean, phone_clean, 
            city, state, zip, age_years, created_date
        )
        SELECT 
            customer_id,
            CONCAT(first_name, ' ', last_name) as customer_name,
            email_clean,
            phone_clean,
            city,
            state,
            zip,
            EXTRACT(YEAR FROM AGE(CURRENT_DATE, dob_clean)) as age_years,
            created_date_utc::DATE as created_date
        FROM silver.customers;
        '''
    )

    load_product_dimension = SQLExecuteQueryOperator(
        task_id='load_product_dimension',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE gold.dim_product CASCADE;
        
        INSERT INTO gold.dim_product (product_code, product_category, avg_price)
        SELECT 
            product_code,
            CASE 
                WHEN product_code LIKE 'PROD-A%' THEN 'Electronics'
                WHEN product_code LIKE 'PROD-B%' THEN 'Clothing'
                WHEN product_code LIKE 'PROD-C%' THEN 'Books'
                WHEN product_code LIKE 'PROD-D%' THEN 'Home & Garden'
                WHEN product_code LIKE 'PROD-E%' THEN 'Sports'
                ELSE 'Other'
            END as product_category,
            AVG(price_per_unit_clean) as avg_price
        FROM silver.orders
        WHERE product_code IS NOT NULL
        GROUP BY product_code;
        '''
    )

    load_date_dimension = SQLExecuteQueryOperator(
        task_id='load_date_dimension',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE gold.dim_date CASCADE;
        
        INSERT INTO gold.dim_date (
            date_key, full_date, year, month, day, quarter, day_of_week, month_name
        )
        SELECT DISTINCT
            TO_CHAR(order_date_utc::DATE, 'YYYYMMDD')::INTEGER as date_key,
            order_date_utc::DATE as full_date,
            EXTRACT(YEAR FROM order_date_utc) as year,
            EXTRACT(MONTH FROM order_date_utc) as month,
            EXTRACT(DAY FROM order_date_utc) as day,
            EXTRACT(QUARTER FROM order_date_utc) as quarter,
            EXTRACT(DOW FROM order_date_utc) as day_of_week,
            TO_CHAR(order_date_utc, 'Month') as month_name
        FROM silver.orders
        WHERE order_date_utc IS NOT NULL;
        '''
    )

    load_sales_facts = SQLExecuteQueryOperator(
        task_id='load_sales_facts',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE gold.fact_sales CASCADE;
        
        INSERT INTO gold.fact_sales (
            order_id, customer_key, product_key, order_date_key,
            quantity, unit_price, total_amount
        )
        SELECT 
            o.order_id,
            dc.customer_key,
            dp.product_key,
            dd.date_key,
            o.quantity_clean,
            o.price_per_unit_clean,
            (o.quantity_clean * o.price_per_unit_clean) as total_amount
        FROM silver.orders o
        JOIN gold.dim_customer dc ON o.customer_id = dc.customer_id
        JOIN gold.dim_product dp ON o.product_code = dp.product_code
        JOIN gold.dim_date dd ON TO_CHAR(o.order_date_utc::DATE, 'YYYYMMDD')::INTEGER = dd.date_key
        WHERE o.quantity_clean IS NOT NULL 
          AND o.price_per_unit_clean IS NOT NULL;
        '''
    )

    build_customer_analytics = SQLExecuteQueryOperator(
        task_id='build_customer_analytics',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE gold.customer_analytics;
        
        INSERT INTO gold.customer_analytics (
            customer_key, total_orders, total_spent, avg_order_value,
            first_order_date, last_order_date, customer_lifetime_days
        )
        SELECT 
            fs.customer_key,
            COUNT(fs.order_id) as total_orders,
            SUM(fs.total_amount) as total_spent,
            AVG(fs.total_amount) as avg_order_value,
            MIN(dd.full_date) as first_order_date,
            MAX(dd.full_date) as last_order_date,
            MAX(dd.full_date) - MIN(dd.full_date) as customer_lifetime_days
        FROM gold.fact_sales fs
        JOIN gold.dim_date dd ON fs.order_date_key = dd.date_key
        GROUP BY fs.customer_key;
        '''
    )

    # Set dependencies as shown in diagram
    create_gold_tables >> [load_customer_dimension, load_product_dimension, load_date_dimension]
    [load_customer_dimension, load_product_dimension, load_date_dimension] >> load_sales_facts
    load_sales_facts >> build_customer_analytics
