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
    dag_id='test_postgresql_connection',
    default_args=default_args,
    description='Test PostgreSQL connection - simple test',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'postgresql']
):

    test_connection = SQLExecuteQueryOperator(
        task_id='test_connection',
        conn_id='postgres_default',
        sql='SELECT 1 as test;'
    )
