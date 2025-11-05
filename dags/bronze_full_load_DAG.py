import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

DATA_DIR = '/opt/airflow/data/bronze_src'

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_erp_orders():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # Read ERP orders CSV
    df = pd.read_csv(os.path.join(DATA_DIR, 'erp/orders.csv'))
    
    # Load to bronze table
    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE bronze.erp_orders_raw')
        df.to_sql(
            name='erp_orders_raw', 
            schema='bronze',
            con=conn, 
            if_exists='append', 
            index=False,
            method='multi'
        )
    print(f'Loaded {len(df)} ERP order records to bronze.erp_orders_raw')

def load_crm_customers():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # Read CRM customers CSV
    df = pd.read_csv(os.path.join(DATA_DIR, 'crm/customers.csv'))
    
    # Load to bronze table
    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE bronze.crm_customers_raw')
        df.to_sql(
            name='crm_customers_raw', 
            schema='bronze',
            con=conn, 
            if_exists='append', 
            index=False,
            method='multi'
        )
    print(f'Loaded {len(df)} CRM customer records to bronze.crm_customers_raw')

with DAG(
    dag_id='bronze_full_load_DAG',
    default_args=default_args,
    description='Bronze Layer Data Loading - Load ERP/CRM CSV files to Raw Tables',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'load', 'csv', 'erp', 'crm']
):

    # Initialize schemas and bronze tables
    init_schemas = SQLExecuteQueryOperator(
        task_id='init_schemas',
        conn_id='postgres_default',
        sql='/opt/airflow/include/sql/init_schemas_pg.sql'
    )
    
    create_bronze_tables = SQLExecuteQueryOperator(
        task_id='create_bronze_tables',
        conn_id='postgres_default',
        sql='''
        CREATE TABLE IF NOT EXISTS bronze.erp_orders_raw (
            order_id VARCHAR(50),
            customer_id VARCHAR(50),
            product_code VARCHAR(50),
            quantity INTEGER,
            price_per_unit DECIMAL(10,2),
            order_date TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS bronze.crm_customers_raw (
            customer_id VARCHAR(50),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(50),
            address VARCHAR(300),
            city VARCHAR(100),
            state VARCHAR(50),
            zip VARCHAR(20),
            dob DATE,
            created_date TIMESTAMP
        );
        '''
    )

    # Load ERP data
    load_erp_data = PythonOperator(
        task_id='load_erp_orders',
        python_callable=load_erp_orders
    )

    # Load CRM data
    load_crm_data = PythonOperator(
        task_id='load_crm_customers',
        python_callable=load_crm_customers
    )

    # Set dependencies as shown in diagram
    init_schemas >> create_bronze_tables >> [load_erp_data, load_crm_data]
