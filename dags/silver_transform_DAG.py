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
    dag_id='silver_transform_DAG',
    default_args=default_args,
    description='Silver Layer Data Transformation - Clean and standardize bronze data',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'transform', 'clean', 'standardize']
):

    create_silver_tables = SQLExecuteQueryOperator(
        task_id='create_silver_tables',
        conn_id='postgres_default',
        sql='''
        -- Create Silver Orders Table (Cleaned)
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            product_code VARCHAR(50), 
            quantity_clean INTEGER,
            price_per_unit_clean DECIMAL(10,2),
            order_date_utc TIMESTAMP,
            processed_date_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create Silver Customers Table (Cleaned)
        CREATE TABLE IF NOT EXISTS silver.customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email_clean VARCHAR(200),
            phone_clean VARCHAR(50),
            city VARCHAR(100),
            state VARCHAR(50),
            zip VARCHAR(20),
            dob_clean DATE,
            created_date_utc TIMESTAMP
        );
        '''
    )

    transform_customers = SQLExecuteQueryOperator(
        task_id='transform_customers',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE silver.customers;
        
        INSERT INTO silver.customers (
            customer_id, first_name, last_name, email_clean, phone_clean, 
            city, state, zip, dob_clean, created_date_utc
        )
        SELECT 
            TRIM(customer_id) as customer_id,
            TRIM(INITCAP(first_name)) as first_name,
            TRIM(INITCAP(last_name)) as last_name,
            LOWER(TRIM(email)) as email_clean,
            REGEXP_REPLACE(TRIM(phone), '[^0-9-]', '', 'g') as phone_clean,
            TRIM(INITCAP(city)) as city,
            UPPER(TRIM(state)) as state,
            TRIM(zip) as zip,
            dob::DATE as dob_clean,
            created_date::TIMESTAMP as created_date_utc
        FROM bronze.crm_customers_raw
        WHERE customer_id IS NOT NULL 
          AND email IS NOT NULL;
        '''
    )

    transform_orders = SQLExecuteQueryOperator(
        task_id='transform_orders',
        conn_id='postgres_default',
        sql='''
        TRUNCATE TABLE silver.orders;
        
        INSERT INTO silver.orders (
            order_id, customer_id, product_code, quantity_clean, 
            price_per_unit_clean, order_date_utc
        )
        SELECT 
            TRIM(order_id) as order_id,
            TRIM(customer_id) as customer_id,
            UPPER(TRIM(product_code)) as product_code,
            CASE 
                WHEN quantity > 0 AND quantity < 1000 THEN quantity
                ELSE NULL 
            END as quantity_clean,
            CASE 
                WHEN price_per_unit > 0 AND price_per_unit < 10000 THEN price_per_unit
                ELSE NULL 
            END as price_per_unit_clean,
            order_date::TIMESTAMP as order_date_utc
        FROM bronze.erp_orders_raw
        WHERE order_id IS NOT NULL 
          AND customer_id IS NOT NULL
          AND quantity > 0 
          AND price_per_unit > 0;
        '''
    )

    # Dependencies: Create tables first, then transform data
    create_silver_tables >> [transform_customers, transform_orders]
