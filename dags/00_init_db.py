from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="init_db_schemas_pg",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="Create Medallion schemas (bronze/silver/gold) - PostgreSQL Version",
    tags=['setup', 'database', 'medallion', 'postgresql']
):
    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id="postgres_default",
        sql="""
        -- PostgreSQL version - Create schemas
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        """
    )