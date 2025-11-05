from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="99_pipeline_full_refresh_pg",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    doc_md="Full medallion pipeline refresh (Bronze->Silver->Gold) - PostgreSQL Version",
    tags=['pipeline', 'full-refresh', 'medallion', 'postgresql']
):

    # Initialize schemas
    init_schemas = SQLExecuteQueryOperator(
        task_id="init_schemas",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/init_schemas_pg.sql"
    )

    # Bronze layer - DDL
    create_bronze_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_tables",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/bronze_ddl_pg.sql"
    )

    # Load sample data (placeholder - would be real ETL in production)
    load_sample_data = SQLExecuteQueryOperator(
        task_id="load_sample_data",
        conn_id="postgres_default",
        sql="""
        -- Sample CRM data
        INSERT INTO bronze.crm_customers_raw (customer_id, first_name, last_name, email, phone, city, state, zip, dob, created_date) VALUES
        (1, 'John', 'Doe', 'john.doe@email.com', '555-0101', 'New York', 'NY', '10001', '1985-03-15', '2023-01-15 10:30:00'),
        (2, 'Jane', 'Smith', 'jane.smith@email.com', '555-0102', 'Los Angeles', 'CA', '90210', '1990-07-22', '2023-02-20 14:15:00'),
        (3, 'Bob', 'Johnson', 'bob.johnson@email.com', '555-0103', 'Chicago', 'IL', '60601', '1978-11-08', '2023-03-10 09:45:00');

        -- Sample ERP order data  
        INSERT INTO bronze.erp_orders_raw (order_id, customer_id, product_code, quantity, price_per_unit, order_date, processed_date) VALUES
        (101, 1, 'WIDGET-A', '2', '25.99', '2023-06-01 12:00:00', '2023-06-01 13:30:00'),
        (102, 1, 'WIDGET-B', '1', '45.50', '2023-06-15 16:20:00', '2023-06-15 17:00:00'),
        (103, 2, 'WIDGET-A', '3', '25.99', '2023-06-10 10:15:00', '2023-06-10 11:00:00'),
        (104, 3, 'WIDGET-C', '1', '99.99', '2023-06-20 14:30:00', '2023-06-20 15:15:00');
        """
    )

    # Silver layer - DDL
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
                WHEN dob ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN dob::date
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
                WHEN quantity ~ '^\\d+$' AND quantity::int > 0 THEN quantity::int
                ELSE 1 
            END as quantity_clean,
            CASE 
                WHEN price_per_unit ~ '^\\d+\\.?\\d*$' AND price_per_unit::numeric > 0 
                THEN price_per_unit::numeric(10,2)
                ELSE 0.00 
            END as price_per_unit_clean,
            order_date AT TIME ZONE 'UTC' as order_date_utc,
            processed_date AT TIME ZONE 'UTC' as processed_date_utc
        FROM bronze.erp_orders_raw
        WHERE order_id IS NOT NULL
        """
    )

    # Gold layer - DDL
    create_gold_tables = SQLExecuteQueryOperator(
        task_id="create_gold_tables",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/gold_model_pg.sql"
    )

    # Gold analytics
    build_customer_analytics = SQLExecuteQueryOperator(
        task_id="build_customer_analytics",
        conn_id="postgres_default",
        sql="""
        INSERT INTO gold.customer_analytics (
            customer_id, first_name, last_name, email_clean,
            total_orders, total_amount_spent, avg_order_value,
            first_order_date, last_order_date, customer_lifetime_days
        )
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.email_clean,
            COUNT(o.order_id) as total_orders,
            COALESCE(SUM(o.quantity_clean * o.price_per_unit_clean), 0) as total_amount_spent,
            CASE 
                WHEN COUNT(o.order_id) > 0 
                THEN ROUND(SUM(o.quantity_clean * o.price_per_unit_clean) / COUNT(o.order_id), 2)
                ELSE 0 
            END as avg_order_value,
            MIN(o.order_date_utc) as first_order_date,
            MAX(o.order_date_utc) as last_order_date,
            CASE 
                WHEN MIN(o.order_date_utc) IS NOT NULL AND MAX(o.order_date_utc) IS NOT NULL
                THEN EXTRACT(DAY FROM (MAX(o.order_date_utc) - MIN(o.order_date_utc)))
                ELSE 0 
            END as customer_lifetime_days
        FROM silver.customers c
        LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email_clean
        """
    )

    build_product_performance = SQLExecuteQueryOperator(
        task_id="build_product_performance",
        conn_id="postgres_default",
        sql="""
        INSERT INTO gold.product_performance (
            product_code, total_quantity_sold, total_revenue,
            unique_customers, avg_price_per_unit, order_frequency
        )
        SELECT 
            product_code,
            SUM(quantity_clean) as total_quantity_sold,
            SUM(quantity_clean * price_per_unit_clean) as total_revenue,
            COUNT(DISTINCT customer_id) as unique_customers,
            ROUND(AVG(price_per_unit_clean), 2) as avg_price_per_unit,
            COUNT(*) as order_frequency
        FROM silver.orders
        WHERE product_code IS NOT NULL
        GROUP BY product_code
        """
    )

    build_monthly_sales = SQLExecuteQueryOperator(
        task_id="build_monthly_sales",
        conn_id="postgres_default",
        sql="""
        INSERT INTO gold.monthly_sales_summary (
            year_month, total_orders, total_revenue, unique_customers, avg_order_value
        )
        SELECT 
            TO_CHAR(order_date_utc, 'YYYY-MM') as year_month,
            COUNT(*) as total_orders,
            SUM(quantity_clean * price_per_unit_clean) as total_revenue,
            COUNT(DISTINCT customer_id) as unique_customers,
            ROUND(AVG(quantity_clean * price_per_unit_clean), 2) as avg_order_value
        FROM silver.orders
        WHERE order_date_utc IS NOT NULL
        GROUP BY TO_CHAR(order_date_utc, 'YYYY-MM')
        ORDER BY year_month
        """
    )

    # Pipeline dependencies
    init_schemas >> create_bronze_tables >> load_sample_data
    load_sample_data >> create_silver_tables >> [transform_customers, transform_orders]
    [transform_customers, transform_orders] >> create_gold_tables
    create_gold_tables >> [build_customer_analytics, build_product_performance, build_monthly_sales]