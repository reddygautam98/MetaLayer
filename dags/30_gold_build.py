from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="gold_build_pg",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md="Build Gold layer (analytics-ready) - PostgreSQL Version",
    tags=['gold', 'analytics', 'postgresql']
):

    create_gold_tables = SQLExecuteQueryOperator(
        task_id="create_gold_tables",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/gold_model_pg.sql"
    )

    # Build customer analytics
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

    # Build product performance
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

    # Build monthly sales summary
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

    # Set up dependencies
    create_gold_tables >> [
        build_customer_analytics, 
        build_product_performance, 
        build_monthly_sales
    ]