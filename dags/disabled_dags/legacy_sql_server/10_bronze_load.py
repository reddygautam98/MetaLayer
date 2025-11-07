import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

DATA_DIR = (
    "' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'data/bronze_src"
)


def load_csv_to_table(csv_path: str, table: str):
    hook = MsSqlHook(mssql_conn_id="mssql_default")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_csv(csv_path)
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {table}")
        df.to_sql(
            name=table.split(".")[-1],
            schema=table.split(".")[0],
            con=conn,
            if_exists="append",
            index=False,
        )


def load_erp_orders():
    load_csv_to_table(
        os.path.join(DATA_DIR, "erp", "orders.csv"), "bronze.erp_orders_raw"
    )


def load_crm_customers():
    load_csv_to_table(
        os.path.join(DATA_DIR, "crm", "customers.csv"), "bronze.crm_customers_raw"
    )


with DAG(
    dag_id="bronze_full_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="TRUNCATE+INSERT Bronze full load from CSVs",
):
    create_bronze_tables = MsSqlOperator(
        task_id="create_bronze_tables",
        mssql_conn_id="mssql_default",
        sql="' + os.path.join(os.environ.get('AIRFLOW_HOME', '~/airflow'), 'include/sql/bronze_ddl.sql",
    )

    load_erp = PythonOperator(
        task_id="load_erp_orders", python_callable=load_erp_orders
    )
    load_crm = PythonOperator(
        task_id="load_crm_customers", python_callable=load_crm_customers
    )

    create_bronze_tables >> [load_erp, load_crm]
