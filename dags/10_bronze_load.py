import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data/bronze_src"

def load_csv_to_postgres_table(csv_path: str, table: str, schema: str):
    """Load CSV data to PostgreSQL table"""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(csv_path)
    
    # Clear existing data
    with engine.connect() as conn:
        conn.execute(f"TRUNCATE TABLE {schema}.{table}")
        conn.commit()
        
    # Load new data
    df.to_sql(name=table, schema=schema, con=engine, if_exists="append", index=False)

def load_erp_orders():
    load_csv_to_postgres_table(
        os.path.join(DATA_DIR, "erp", "orders.csv"), 
        "erp_orders_raw", 
        "bronze"
    )

def load_crm_customers():
    load_csv_to_postgres_table(
        os.path.join(DATA_DIR, "crm", "customers.csv"), 
        "crm_customers_raw", 
        "bronze"
    )

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_full_load_pg",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="TRUNCATE+INSERT Bronze full load from CSVs - PostgreSQL Version",
    tags=['bronze', 'etl', 'csv', 'postgresql']
):
    create_bronze_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_tables",
        conn_id="postgres_default",
        sql="/opt/airflow/include/sql/bronze_ddl_pg.sql"
    )

    load_erp = PythonOperator(
        task_id="load_erp_orders", 
        python_callable=load_erp_orders
    )
    
    load_crm = PythonOperator(
        task_id="load_crm_customers", 
        python_callable=load_crm_customers
    )

    create_bronze_tables >> [load_erp, load_crm]