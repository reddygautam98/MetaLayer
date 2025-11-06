from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def validate_silver_processing():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    print("🔍 Validating Silver layer transformations...")

    # Get record counts
    sales_count = hook.get_first("SELECT COUNT(*) FROM silver.sales_cleaned")[0]
    customers_count = hook.get_first(
        "SELECT COUNT(*) FROM silver.customers_standardized"
    )[0]

    print(f"📊 Silver Layer Validation:")
    print(f"   Sales Cleaned: {sales_count:,} records")
    print(f"   Customers Standardized: {customers_count:,} records")

    if sales_count >= 1000000 and customers_count >= 1000000:
        print("✅ Silver layer validation PASSED!")
    else:
        print("❌ Silver layer validation FAILED!")
        raise ValueError(
            f"Expected 1M+ records, got Sales: {sales_count}, Customers: {customers_count}"
        )


dag = DAG(
    dag_id="silver_transform_million_records_DAG",
    default_args=default_args,
    description="Silver Layer - Transform 1M+ Records with Data Quality",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "transform", "million-records", "data-quality"],
)

with dag:
    # Create Silver layer tables optimized for 1M+ records
    create_silver_tables = SQLExecuteQueryOperator(
        task_id="create_silver_tables",
        conn_id="postgres_default",
        sql="""
        -- Sales Cleaned Table (Silver Layer)
        DROP TABLE IF EXISTS silver.sales_cleaned CASCADE;
        CREATE TABLE silver.sales_cleaned (
            sales_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            quantity INTEGER CHECK (quantity > 0),
            sale_date DATE NOT NULL,
            sale_amount DECIMAL(12,2) CHECK (sale_amount > 0),
            sale_year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM sale_date)) STORED,
            sale_month INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM sale_date)) STORED,
            revenue_category VARCHAR(20) GENERATED ALWAYS AS (
                CASE 
                    WHEN sale_amount >= 1000 THEN 'HIGH'
                    WHEN sale_amount >= 500 THEN 'MEDIUM'
                    ELSE 'LOW'
                END
            ) STORED
        );
        """,
        dag=dag,
    )

    # Transform ERP Sales data with quality checks
    transform_sales_data = SQLExecuteQueryOperator(
        task_id="transform_sales_data_million_records",
        conn_id="postgres_default",
        sql="""
        INSERT INTO silver.sales_cleaned_million (
            sales_id, product_id, customer_id, quantity, sale_date, sale_amount
        )
        SELECT 
            sales_id,
            product_id,
            customer_id,
            GREATEST(quantity, 1) as quantity,
            sale_date,
            GREATEST(sale_amount, 0.01) as sale_amount
        FROM bronze.erp_sales_raw
        WHERE sales_id IS NOT NULL 
        AND product_id IS NOT NULL 
        AND customer_id IS NOT NULL
        AND sale_date IS NOT NULL
        AND sale_amount > 0
        AND quantity > 0;
        """,
        dag=dag,
    )

    # Transform CRM Customer data with standardization
    transform_customers_data = SQLExecuteQueryOperator(
        task_id="transform_customers_data_million_records",
        conn_id="postgres_default",
        sql="""
        INSERT INTO silver.customers_standardized_million (
            customer_id, customer_name_clean, email_clean, region_clean
        )
        SELECT 
            customer_id,
            TRIM(INITCAP(customer_name)) as customer_name_clean,
            LOWER(TRIM(email)) as email_clean,
            UPPER(TRIM(region)) as region_clean
        FROM bronze.crm_customers_raw
        WHERE customer_id IS NOT NULL
        AND customer_name IS NOT NULL AND TRIM(customer_name) != ''
        AND email IS NOT NULL AND TRIM(email) != '';
        """,
        dag=dag,
    )

    # Data quality validation task
    validate_silver_data = PythonOperator(
        task_id="validate_silver_data_million_records",
        python_callable=validate_silver_processing,
        dag=dag,
    )

    # Task Dependencies - Parallel processing for million records
    create_silver_tables >> [transform_sales_data, transform_customers_data]
    [transform_sales_data, transform_customers_data] >> validate_silver_data
