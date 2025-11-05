from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="silver_transform_pg",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md="Transform Bronze to Silver (cleaned, standardized) - PostgreSQL Version",
    tags=['silver', 'transformation', 'postgresql']
):

    create_silver_tables = SQLExecuteQueryOperator(
        task_id="create_silver_tables",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/silver_transform_pg.sql"
    )

    # Silver transformations
    transform_customers = SQLExecuteQueryOperator(
        task_id="transform_customers",
        conn_id="postgres_default",
        sql="""
        INSERT INTO silver.customers (
            customer_id, first_name, last_name, email_clean, phone_clean, 
            city, state, zip, dob_clean, created_date_utc
        )
        SELECT 
            customer_id,
            TRIM(first_name) as first_name,
            TRIM(last_name) as last_name,
            LOWER(TRIM(email)) as email_clean,
            REGEXP_REPLACE(phone, '[^0-9]', '', 'g') as phone_clean,
            TRIM(city) as city,
            TRIM(state) as state,
            TRIM(zip) as zip,
            CASE 
                WHEN dob ~ '^\d{4}-\d{2}-\d{2}$' THEN dob::date
                ELSE NULL 
            END as dob_clean,
            created_date AT TIME ZONE 'UTC' as created_date_utc
        FROM bronze.crm_customers_raw
        WHERE customer_id IS NOT NULL
        """
    )

    transform_orders = SQLExecuteQueryOperator(
        task_id="transform_orders",
        conn_id="postgres_default",
        sql="""
        INSERT INTO silver.orders (
            order_id, customer_id, product_code, quantity_clean, 
            price_per_unit_clean, order_date_utc, processed_date_utc
        )
        SELECT 
            order_id,
            customer_id,
            UPPER(TRIM(product_code)) as product_code,
            CASE 
                WHEN quantity ~ '^\d+$' AND quantity::int > 0 THEN quantity::int
                ELSE 1 
            END as quantity_clean,
            CASE 
                WHEN price_per_unit ~ '^\d+\.?\d*$' AND price_per_unit::numeric > 0 
                THEN price_per_unit::numeric(10,2)
                ELSE 0.00 
            END as price_per_unit_clean,
            order_date AT TIME ZONE 'UTC' as order_date_utc,
            processed_date AT TIME ZONE 'UTC' as processed_date_utc
        FROM bronze.erp_orders_raw
        WHERE order_id IS NOT NULL
        """
    )

    # Set up dependencies
    create_silver_tables >> [transform_customers, transform_orders]