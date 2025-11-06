from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

default_args = {
    'owner': 'data-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="init_db_schemas",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="Create Medallion schemas (bronze/silver/gold)",
    tags=['setup', 'database', 'medallion']
):
    create_schemas = MsSqlOperator(
        task_id="create_schemas",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/init_schemas.sql",
        autocommit=True
    )
