from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "analytics-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def generate_analytics_report():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    print("📊 Generating Analytics Report for 1M+ Records...")
    
    # Key business metrics
    total_revenue = hook.get_first("SELECT SUM(total_revenue) FROM gold.monthly_sales_summary")[0]
    total_customers = hook.get_first("SELECT COUNT(DISTINCT customer_id) FROM silver.customers_standardized")[0]
    total_orders = hook.get_first("SELECT SUM(total_transactions) FROM gold.monthly_sales_summary")[0]
    
    print(f"💰 Business Performance:")
    print(f"   Total Revenue: ${float(total_revenue):,.2f}")
    print(f"   Total Customers: {total_customers:,}")
    print(f"   Total Orders: {total_orders:,}")
    print(f"   Revenue per Customer: ${float(total_revenue)/total_customers:,.2f}")
    
    # Regional performance
    regions = hook.get_records("SELECT region_clean, total_revenue FROM gold.regional_performance ORDER BY total_revenue DESC LIMIT 5")
    print(f"🌍 Top 5 Regions by Revenue:")
    for region, revenue in regions:
        print(f"   {region}: ${float(revenue):,.2f}")
    
    print("✅ Analytics report generated successfully!")

with DAG(
    dag_id="gold_analytics_million_records_DAG",
    default_args=default_args,
    description="Gold Layer - Analytics & BI Ready Tables for 1M+ Records",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "analytics", "million-records", "bi-ready"]
):

    # Create Gold layer analytics tables
    create_gold_tables = SQLExecuteQueryOperator(
        task_id="create_gold_analytics_tables",
        conn_id="postgres_default",
        sql="""
        -- Monthly Sales Summary (Gold Layer)
        DROP TABLE IF EXISTS gold.monthly_sales_summary CASCADE;
        CREATE TABLE gold.monthly_sales_summary AS
        SELECT 
            sale_year,
            sale_month,
            COUNT(*) as total_transactions,
            SUM(sale_amount) as total_revenue,
            AVG(sale_amount) as avg_sale_amount,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            CURRENT_TIMESTAMP as created_at
        FROM silver.sales_cleaned
        GROUP BY sale_year, sale_month;
        
        -- Regional Performance (Gold Layer)
        DROP TABLE IF EXISTS gold.regional_performance CASCADE;
        CREATE TABLE gold.regional_performance AS
        SELECT 
            c.region_clean,
            COUNT(DISTINCT c.customer_id) as total_customers,
            COUNT(s.sales_id) as total_transactions,
            SUM(s.sale_amount) as total_revenue,
            AVG(s.sale_amount) as avg_transaction_value,
            SUM(s.sale_amount) / COUNT(DISTINCT c.customer_id) as revenue_per_customer,
            CURRENT_TIMESTAMP as created_at
        FROM silver.customers_standardized c
        JOIN silver.sales_cleaned s ON c.customer_id = s.customer_id  
        GROUP BY c.region_clean;
        
        -- Product Performance (Gold Layer)
        DROP TABLE IF EXISTS gold.product_performance CASCADE;
        CREATE TABLE gold.product_performance AS
        SELECT 
            product_id,
            COUNT(*) as total_sales,
            SUM(sale_amount) as total_revenue,
            SUM(quantity) as total_quantity_sold,
            AVG(sale_amount) as avg_sale_price,
            COUNT(DISTINCT customer_id) as unique_buyers,
            RANK() OVER (ORDER BY SUM(sale_amount) DESC) as revenue_rank,
            CURRENT_TIMESTAMP as created_at
        FROM silver.sales_cleaned
        GROUP BY product_id;
        
        -- Create indexes for analytics performance
        CREATE INDEX idx_gold_monthly_year_month ON gold.monthly_sales_summary(sale_year, sale_month);
        CREATE INDEX idx_gold_regional_revenue ON gold.regional_performance(total_revenue DESC);
        CREATE INDEX idx_gold_product_revenue ON gold.product_performance(total_revenue DESC);
        """
    )

    # Create BI-ready views
    create_bi_views = SQLExecuteQueryOperator(
        task_id="create_bi_ready_views",
        conn_id="postgres_default",
        sql="""
        -- Customer 360 View
        CREATE OR REPLACE VIEW gold.customer_360_view AS
        SELECT 
            c.customer_id,
            c.customer_name_clean,
            c.email_clean,
            c.region_clean,
            COUNT(s.sales_id) as total_orders,
            SUM(s.sale_amount) as lifetime_value,
            AVG(s.sale_amount) as avg_order_value,
            MIN(s.sale_date) as first_purchase,
            MAX(s.sale_date) as last_purchase,
            COUNT(DISTINCT s.product_id) as unique_products_purchased
        FROM silver.customers_standardized c
        LEFT JOIN silver.sales_cleaned s ON c.customer_id = s.customer_id
        GROUP BY c.customer_id, c.customer_name_clean, c.email_clean, c.region_clean;
        
        -- Sales Trends View
        CREATE OR REPLACE VIEW gold.sales_trends_view AS
        SELECT 
            sale_year,
            sale_month,
            total_revenue,
            total_transactions,
            avg_sale_amount,
            LAG(total_revenue) OVER (ORDER BY sale_year, sale_month) as prev_month_revenue,
            ROUND(
                ((total_revenue - LAG(total_revenue) OVER (ORDER BY sale_year, sale_month)) 
                / LAG(total_revenue) OVER (ORDER BY sale_year, sale_month) * 100), 2
            ) as revenue_growth_pct
        FROM gold.monthly_sales_summary
        ORDER BY sale_year, sale_month;
        """
    )

    # Generate analytics report
    generate_report = PythonOperator(
        task_id="generate_analytics_report",
        python_callable=generate_analytics_report
    )

    # Set dependencies
    create_gold_tables >> create_bi_views >> generate_report
