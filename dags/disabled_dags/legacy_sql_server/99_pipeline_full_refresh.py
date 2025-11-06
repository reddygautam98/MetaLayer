import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator

DATA_DIR = (
    "' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'data/bronze_src"
)


def load_table(csv_rel_path, target):
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    engine = hook.get_sqlalchemy_engine()
    p = os.path.join(DATA_DIR, csv_rel_path)
    df = pd.read_csv(p)
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {target}")
        df.to_sql(
            name=target.split(".")[-1],
            schema=target.split(".")[0],
            con=conn,
            if_exists="append",
            index=False,
        )


with DAG(
    dag_id="pipeline_full_refresh",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="Bronze→Silver→Gold full refresh",
):
    init_schemas = MsSqlOperator(
        task_id="init_schemas",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/init_schemas.sql",
    )
    create_bronze = MsSqlOperator(
        task_id="create_bronze",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/bronze_ddl.sql",
    )
    bronze_erp = PythonOperator(
        task_id="bronze_erp",
        python_callable=lambda: load_table("erp/orders.csv", "bronze.erp_orders_raw"),
    )
    bronze_crm = PythonOperator(
        task_id="bronze_crm",
        python_callable=lambda: load_table(
            "crm/customers.csv", "bronze.crm_customers_raw"
        ),
    )
    silver = MsSqlOperator(
        task_id="silver",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/silver_transform.sql",
    )
    gold = MsSqlOperator(
        task_id="gold",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/gold_model.sql",
    )

    init_schemas >> create_bronze >> [bronze_erp, bronze_crm] >> silver >> gold
