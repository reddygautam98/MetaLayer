from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def pipeline_start_notification():
    print("🚀 Starting Million Records Pipeline...")
    print("   📁 Loading 1M ERP Sales records")
    print("   👥 Loading 1M CRM Customer records") 
    print("   🔄 Processing Bronze → Silver → Gold layers")
    print("   📊 Generating enterprise analytics")

def pipeline_completion_report():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    print("🎉 Million Records Pipeline COMPLETED!")
    print("=" * 60)
    
    # Bronze layer stats
    bronze_sales = hook.get_first("SELECT COUNT(*) FROM bronze.erp_sales_raw")[0]
    bronze_customers = hook.get_first("SELECT COUNT(*) FROM bronze.crm_customers_raw")[0]
    
    # Silver layer stats  
    silver_sales = hook.get_first("SELECT COUNT(*) FROM silver.sales_cleaned")[0]
    silver_customers = hook.get_first("SELECT COUNT(*) FROM silver.customers_standardized")[0]
    
    # Gold layer stats
    gold_monthly = hook.get_first("SELECT COUNT(*) FROM gold.monthly_sales_summary")[0]
    gold_regional = hook.get_first("SELECT COUNT(*) FROM gold.regional_performance")[0]
    gold_products = hook.get_first("SELECT COUNT(*) FROM gold.product_performance")[0]
    
    print(f"📊 PIPELINE SUMMARY:")
    print(f"   Bronze Layer: {bronze_sales:,} sales + {bronze_customers:,} customers")
    print(f"   Silver Layer: {silver_sales:,} sales + {silver_customers:,} customers") 
    print(f"   Gold Layer: {gold_monthly} months + {gold_regional} regions + {gold_products:,} products")
    
    # Calculate total revenue
    total_revenue = hook.get_first("SELECT SUM(total_revenue) FROM gold.monthly_sales_summary")[0]
    print(f"   💰 Total Revenue Processed: ${float(total_revenue):,.2f}")
    
    print("✅ Enterprise data pipeline ready for BI & Analytics!")

with DAG(
    dag_id="master_million_records_pipeline_DAG",
    default_args=default_args,
    description="Master Pipeline - Complete 1M Records Bronze→Silver→Gold Processing",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["master", "pipeline", "million-records", "enterprise", "medallion"]
):

    # Pipeline start notification
    start_notification = PythonOperator(
        task_id="pipeline_start_notification",
        python_callable=pipeline_start_notification
    )

    # Trigger Bronze layer data loading (1M records)
    trigger_bronze_load = TriggerDagRunOperator(
        task_id="trigger_bronze_million_records_load",
        trigger_dag_id="load_million_records_DAG",
        wait_for_completion=True,
        poke_interval=30
    )

    # Trigger Silver layer transformations
    trigger_silver_transform = TriggerDagRunOperator(
        task_id="trigger_silver_million_records_transform",
        trigger_dag_id="silver_transform_million_records_DAG", 
        wait_for_completion=True,
        poke_interval=30
    )

    # Trigger Gold layer analytics
    trigger_gold_analytics = TriggerDagRunOperator(
        task_id="trigger_gold_million_records_analytics",
        trigger_dag_id="gold_analytics_million_records_DAG",
        wait_for_completion=True,
        poke_interval=30
    )

    # Pipeline completion report
    completion_report = PythonOperator(
        task_id="pipeline_completion_report",
        python_callable=pipeline_completion_report
    )

    # Set pipeline dependencies
    start_notification >> trigger_bronze_load >> trigger_silver_transform >> trigger_gold_analytics >> completion_report
