from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(
    dag_id="silver_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="Clean & standardize data into Silver",
):
    silver_sql = MsSqlOperator(
        task_id="run_silver_sql",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/silver_transform.sql",
    )
