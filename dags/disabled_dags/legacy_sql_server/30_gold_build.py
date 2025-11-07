from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

with DAG(
    dag_id="gold_build",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="Business-ready Gold layer (dimensions, facts, rules)",
):
    gold_sql = MsSqlOperator(
        task_id="run_gold_sql",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/gold_model.sql",
    )
