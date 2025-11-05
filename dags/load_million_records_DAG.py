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

def load_erp_sales_data():
    '''Load 1M ERP sales records from CSV to bronze layer'''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    print('🚀 Loading ERP Sales data (1M+ records)...')
    
    # Read ERP sales CSV in chunks for memory efficiency
    csv_path = os.path.join(DATA_DIR, 'erp/erp_sales.csv')
    chunk_size = 50000  # Process 50K records at a time
    
    total_records = 0
    chunk_num = 1
    
    with engine.begin() as conn:
        # Clear existing data
        conn.execute('TRUNCATE TABLE bronze.erp_sales_raw')
        
        # Process CSV in chunks
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            print(f'   Processing chunk {chunk_num}: {len(chunk_df):,} records')
            
            chunk_df.to_sql(
                name='erp_sales_raw',
                schema='bronze', 
                con=conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            total_records += len(chunk_df)
            chunk_num += 1
    
    print(f'✅ ERP Sales: {total_records:,} records loaded successfully!')
    return total_records

def load_crm_customers_data():
    '''Load 1M CRM customer records from CSV to bronze layer'''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    print('🚀 Loading CRM Customer data (1M+ records)...')
    
    # Read CRM customers CSV in chunks for memory efficiency  
    csv_path = os.path.join(DATA_DIR, 'crm/crm_customers.csv')
    chunk_size = 50000  # Process 50K records at a time
    
    total_records = 0
    chunk_num = 1
    
    with engine.begin() as conn:
        # Clear existing data
        conn.execute('TRUNCATE TABLE bronze.crm_customers_raw')
        
        # Process CSV in chunks
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            print(f'   Processing chunk {chunk_num}: {len(chunk_df):,} records')
            
            chunk_df.to_sql(
                name='crm_customers_raw',
                schema='bronze',
                con=conn, 
                if_exists='append',
                index=False,
                method='multi'
            )
            
            total_records += len(chunk_df)
            chunk_num += 1
    
    print(f'✅ CRM Customers: {total_records:,} records loaded successfully!')
    return total_records

def validate_data_load():
    '''Validate that all data was loaded correctly'''
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    print('🔍 Validating data load...')
    
    # Get record counts
    sales_count = hook.get_first('SELECT COUNT(*) FROM bronze.erp_sales_raw')[0]
    customers_count = hook.get_first('SELECT COUNT(*) FROM bronze.crm_customers_raw')[0]
    
    print(f'📊 Bronze Layer Validation:')
    print(f'   ERP Sales: {sales_count:,} records')
    print(f'   CRM Customers: {customers_count:,} records')
    print(f'   Total Records: {sales_count + customers_count:,}')
    
    # Sample data validation
    sample_sale = hook.get_first('SELECT sales_id, product_id, customer_id, sale_amount FROM bronze.erp_sales_raw LIMIT 1')
    sample_customer = hook.get_first('SELECT customer_id, customer_name, email, region FROM bronze.crm_customers_raw LIMIT 1')
    
    print(f'📋 Sample Records:')
    print(f'   Sale: {sample_sale}')
    print(f'   Customer: {sample_customer}')
    
    # Data quality checks
    null_sales = hook.get_first('SELECT COUNT(*) FROM bronze.erp_sales_raw WHERE sales_id IS NULL')[0]
    null_customers = hook.get_first('SELECT COUNT(*) FROM bronze.crm_customers_raw WHERE customer_id IS NULL')[0]
    
    print(f'🔍 Data Quality:')
    print(f'   Null Sales IDs: {null_sales}')
    print(f'   Null Customer IDs: {null_customers}')
    
    if sales_count >= 1000000 and customers_count >= 1000000:
        print('✅ Data load validation PASSED!')
    else:
        print('❌ Data load validation FAILED!')
        raise ValueError(f'Expected 1M+ records, got Sales: {sales_count}, Customers: {customers_count}')

with DAG(
    dag_id='load_million_records_DAG',
    default_args=default_args,
    description='Load 1 Million+ Records - ERP Sales & CRM Customers to Bronze Layer',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'load', 'million-records', 'enterprise', 'big-data']
):

    # Initialize schemas and bronze tables with updated structure
    init_schemas = SQLExecuteQueryOperator(
        task_id='init_schemas',
        conn_id='postgres_default',
        sql='''
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        '''
    )
    
    create_bronze_tables = SQLExecuteQueryOperator(
        task_id='create_bronze_tables',
        conn_id='postgres_default',
        sql='''
        -- ERP Sales Bronze Table (Updated Structure)
        DROP TABLE IF EXISTS bronze.erp_sales_raw CASCADE;
        CREATE TABLE bronze.erp_sales_raw (
            sales_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            sale_date DATE,
            sale_amount DECIMAL(10,2),
            loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- CRM Customers Bronze Table (Updated Structure)  
        DROP TABLE IF EXISTS bronze.crm_customers_raw CASCADE;
        CREATE TABLE bronze.crm_customers_raw (
            customer_id INTEGER,
            customer_name VARCHAR(200),
            email VARCHAR(200),
            region VARCHAR(50),
            loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for performance on large datasets
        CREATE INDEX IF NOT EXISTS idx_erp_sales_customer_id ON bronze.erp_sales_raw(customer_id);
        CREATE INDEX IF NOT EXISTS idx_erp_sales_product_id ON bronze.erp_sales_raw(product_id);
        CREATE INDEX IF NOT EXISTS idx_erp_sales_date ON bronze.erp_sales_raw(sale_date);
        CREATE INDEX IF NOT EXISTS idx_crm_customer_id ON bronze.crm_customers_raw(customer_id);
        CREATE INDEX IF NOT EXISTS idx_crm_region ON bronze.crm_customers_raw(region);
        '''
    )

    # Load 1M ERP sales records
    load_erp_sales = PythonOperator(
        task_id='load_erp_sales_million_records',
        python_callable=load_erp_sales_data
    )

    # Load 1M CRM customer records
    load_crm_customers = PythonOperator(
        task_id='load_crm_customers_million_records', 
        python_callable=load_crm_customers_data
    )
    
    # Validate the data load
    validate_load = PythonOperator(
        task_id='validate_million_records_load',
        python_callable=validate_data_load
    )

    # Set dependencies
    init_schemas >> create_bronze_tables >> [load_erp_sales, load_crm_customers] >> validate_load
