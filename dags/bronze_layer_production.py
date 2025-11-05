import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable

# Configuration
DATA_DIR = '/opt/airflow/data/bronze_src'
CHUNK_SIZE = 50000
MAX_ERRORS = int(Variable.get("max_record_errors", default_var=100))

# Setup logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

class DataQualityError(AirflowException):
    """Custom exception for data quality issues"""
    pass

def validate_csv_file(file_path: str, required_columns: list) -> Dict[str, Any]:
    """Validate CSV file structure and basic quality"""
    try:
        # Check file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Read first chunk to validate structure
        df_sample = pd.read_csv(file_path, nrows=1000)
        
        # Check required columns
        missing_cols = set(required_columns) - set(df_sample.columns)
        if missing_cols:
            raise DataQualityError(f"Missing required columns: {missing_cols}")
        
        # Get file stats
        total_rows = sum(1 for _ in open(file_path)) - 1  # Exclude header
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        validation_result = {
            'file_path': file_path,
            'total_rows': total_rows,
            'file_size_mb': round(file_size_mb, 2),
            'columns': list(df_sample.columns),
            'validation_status': 'PASSED'
        }
        
        logger.info(f"File validation passed: {validation_result}")
        return validation_result
        
    except Exception as e:
        logger.error(f"File validation failed for {file_path}: {str(e)}")
        raise DataQualityError(f"File validation failed: {str(e)}")

def validate_data_quality(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Perform data quality validation on DataFrame"""
    quality_issues = []
    
    # Check for completely null rows
    null_rows = df.isnull().all(axis=1).sum()
    if null_rows > 0:
        quality_issues.append(f"Found {null_rows} completely null rows")
    
    # Check for duplicate rows
    duplicate_rows = df.duplicated().sum()
    if duplicate_rows > 0:
        quality_issues.append(f"Found {duplicate_rows} duplicate rows")
    
    # Table-specific validation
    if table_name == 'erp_sales_raw':
        # Sales-specific validation
        if 'sales_id' in df.columns:
            null_sales_ids = df['sales_id'].isnull().sum()
            if null_sales_ids > 0:
                quality_issues.append(f"Found {null_sales_ids} null sales IDs")
        
        if 'sale_amount' in df.columns:
            negative_amounts = (df['sale_amount'] <= 0).sum()
            if negative_amounts > 0:
                quality_issues.append(f"Found {negative_amounts} negative/zero sale amounts")
    
    elif table_name == 'crm_customers_raw':
        # Customer-specific validation
        if 'customer_id' in df.columns:
            null_customer_ids = df['customer_id'].isnull().sum()
            if null_customer_ids > 0:
                quality_issues.append(f"Found {null_customer_ids} null customer IDs")
        
        if 'email' in df.columns:
            invalid_emails = df['email'].notna() & (~df['email'].str.contains('@', na=False))
            if invalid_emails.sum() > 0:
                quality_issues.append(f"Found {invalid_emails.sum()} invalid email formats")
    
    # Fail if too many issues
    if len(quality_issues) > MAX_ERRORS:
        raise DataQualityError(f"Too many data quality issues: {quality_issues}")
    
    return {
        'table_name': table_name,
        'total_rows': len(df),
        'quality_issues': quality_issues,
        'quality_score': max(0, 100 - len(quality_issues))
    }

def load_csv_with_validation(csv_path: str, table_name: str, schema: str, required_columns: list) -> Dict[str, Any]:
    """Load CSV to PostgreSQL with comprehensive validation and error handling"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Step 1: Validate file
        file_validation = validate_csv_file(csv_path, required_columns)
        logger.info(f"Starting load of {file_validation['total_rows']} records to {schema}.{table_name}")
        
        # Step 2: Get SQLAlchemy engine
        engine = hook.get_sqlalchemy_engine()
        
        # Step 3: Begin transaction
        with engine.begin() as transaction:
            # Clear existing data (atomic operation within transaction)
            transaction.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            logger.info(f"Truncated {schema}.{table_name}")
            
            # Step 4: Process in chunks with validation
            total_loaded = 0
            chunk_num = 1
            quality_summary = []
            
            for chunk_df in pd.read_csv(csv_path, chunksize=CHUNK_SIZE):
                logger.info(f"Processing chunk {chunk_num}: {len(chunk_df)} records")
                
                # Validate chunk quality
                quality_result = validate_data_quality(chunk_df, table_name)
                quality_summary.append(quality_result)
                
                # Clean data (basic cleaning)
                chunk_df = chunk_df.dropna(how='all')  # Remove completely null rows
                chunk_df = chunk_df.drop_duplicates()  # Remove duplicates
                
                # Load chunk to database
                chunk_df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=transaction,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_loaded += len(chunk_df)
                chunk_num += 1
                
                # Log progress every 10 chunks
                if chunk_num % 10 == 0:
                    logger.info(f"Loaded {total_loaded:,} records so far...")
        
        # Step 5: Verify final load
        final_count = hook.get_first(f"SELECT COUNT(*) FROM {schema}.{table_name}")[0]
        
        load_result = {
            'table_name': f"{schema}.{table_name}",
            'source_file': csv_path,
            'records_processed': file_validation['total_rows'],
            'records_loaded': final_count,
            'file_size_mb': file_validation['file_size_mb'],
            'chunks_processed': chunk_num - 1,
            'quality_summary': quality_summary,
            'load_status': 'SUCCESS'
        }
        
        logger.info(f"Load completed successfully: {load_result}")
        return load_result
        
    except Exception as e:
        logger.error(f"Load failed for {csv_path} -> {schema}.{table_name}: {str(e)}")
        raise AirflowException(f"Data load failed: {str(e)}")

def load_erp_sales_data():
    """Load ERP sales data with validation"""
    csv_path = os.path.join(DATA_DIR, 'erp/erp_sales.csv')
    required_columns = ['sales_id', 'product_id', 'customer_id', 'quantity', 'sale_date', 'sale_amount']
    
    return load_csv_with_validation(
        csv_path=csv_path,
        table_name='erp_sales_raw',
        schema='bronze',
        required_columns=required_columns
    )

def load_crm_customers_data():
    """Load CRM customers data with validation"""
    csv_path = os.path.join(DATA_DIR, 'crm/crm_customers.csv')
    required_columns = ['customer_id', 'customer_name', 'email', 'region']
    
    return load_csv_with_validation(
        csv_path=csv_path,
        table_name='crm_customers_raw',
        schema='bronze',
        required_columns=required_columns
    )

def validate_bronze_layer():
    """Final validation of bronze layer data"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Get record counts
        sales_count = hook.get_first('SELECT COUNT(*) FROM bronze.erp_sales_raw')[0]
        customers_count = hook.get_first('SELECT COUNT(*) FROM bronze.crm_customers_raw')[0]
        
        # Validate minimum expected records
        min_expected_records = 100000  # Adjust based on your requirements
        
        validation_results = {
            'bronze_erp_sales': sales_count,
            'bronze_crm_customers': customers_count,
            'total_records': sales_count + customers_count,
            'validation_timestamp': datetime.utcnow().isoformat()
        }
        
        # Check thresholds
        if sales_count < min_expected_records:
            raise DataQualityError(f"ERP sales count too low: {sales_count} < {min_expected_records}")
        
        if customers_count < min_expected_records:
            raise DataQualityError(f"CRM customers count too low: {customers_count} < {min_expected_records}")
        
        # Sample data validation
        sample_sale = hook.get_first('SELECT sales_id, customer_id, sale_amount FROM bronze.erp_sales_raw WHERE sale_amount > 0 LIMIT 1')
        sample_customer = hook.get_first('SELECT customer_id, customer_name, email FROM bronze.crm_customers_raw WHERE email LIKE \'%@%\' LIMIT 1')
        
        if not sample_sale:
            raise DataQualityError("No valid sales records found")
        
        if not sample_customer:
            raise DataQualityError("No valid customer records found")
        
        validation_results['sample_sale'] = sample_sale
        validation_results['sample_customer'] = sample_customer
        validation_results['validation_status'] = 'PASSED'
        
        logger.info(f"Bronze layer validation completed: {validation_results}")
        return validation_results
        
    except Exception as e:
        logger.error(f"Bronze layer validation failed: {str(e)}")
        raise AirflowException(f"Bronze layer validation failed: {str(e)}")

with DAG(
    dag_id='bronze_layer_production_load',
    default_args=default_args,
    description='Production Bronze Layer - Load ERP Sales & CRM Customers with Data Quality Validation',
    schedule=None,  # Triggered by upstream systems
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'production', 'data-quality', 'validation'],
    doc_md="""
    # Bronze Layer Production Data Loading
    
    This DAG loads raw data from CSV files into the bronze layer with comprehensive:
    - File validation
    - Data quality checks  
    - Error handling
    - Atomic operations
    - Progress monitoring
    
    ## Data Sources:
    - ERP Sales: `/opt/airflow/data/bronze_src/erp/erp_sales.csv`
    - CRM Customers: `/opt/airflow/data/bronze_src/crm/crm_customers.csv`
    
    ## Quality Checks:
    - File structure validation
    - Null value detection
    - Duplicate detection
    - Business rule validation
    - Data type validation
    """
):

    # Initialize schemas and tables
    init_schemas = SQLExecuteQueryOperator(
        task_id='init_database_schemas',
        conn_id='postgres_default',
        sql='''
        -- Create schemas if not exists
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        
        -- Create audit schema for data quality tracking
        CREATE SCHEMA IF NOT EXISTS audit;
        ''',
        doc_md="Initialize database schemas for medallion architecture"
    )
    
    create_bronze_tables = SQLExecuteQueryOperator(
        task_id='create_bronze_tables',
        conn_id='postgres_default',
        sql='''
        -- ERP Sales Bronze Table (Production Schema)
        DROP TABLE IF EXISTS bronze.erp_sales_raw CASCADE;
        CREATE TABLE bronze.erp_sales_raw (
            sales_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            quantity INTEGER CHECK (quantity > 0),
            sale_date DATE NOT NULL,
            sale_amount DECIMAL(12,2) CHECK (sale_amount > 0),
            loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_erp_sales_raw PRIMARY KEY (sales_id)
        );
        
        -- CRM Customers Bronze Table (Production Schema)
        DROP TABLE IF EXISTS bronze.crm_customers_raw CASCADE;
        CREATE TABLE bronze.crm_customers_raw (
            customer_id INTEGER NOT NULL,
            customer_name VARCHAR(200) NOT NULL,
            email VARCHAR(200) CHECK (email LIKE '%@%'),
            region VARCHAR(50) NOT NULL,
            loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT pk_crm_customers_raw PRIMARY KEY (customer_id)
        );
        
        -- Create performance indexes
        CREATE INDEX IF NOT EXISTS idx_erp_sales_customer_id ON bronze.erp_sales_raw(customer_id);
        CREATE INDEX IF NOT EXISTS idx_erp_sales_product_id ON bronze.erp_sales_raw(product_id);
        CREATE INDEX IF NOT EXISTS idx_erp_sales_date ON bronze.erp_sales_raw(sale_date);
        CREATE INDEX IF NOT EXISTS idx_crm_customers_region ON bronze.crm_customers_raw(region);
        CREATE INDEX IF NOT EXISTS idx_crm_customers_email ON bronze.crm_customers_raw(email);
        
        -- Create audit table for tracking loads
        CREATE TABLE IF NOT EXISTS audit.data_load_log (
            load_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            source_file VARCHAR(500),
            records_loaded INTEGER,
            load_start_time TIMESTAMP,
            load_end_time TIMESTAMP,
            load_status VARCHAR(20),
            quality_score INTEGER,
            error_message TEXT
        );
        ''',
        doc_md="Create production bronze tables with constraints and indexes"
    )

    # Load data with validation
    load_erp_sales = PythonOperator(
        task_id='load_erp_sales_with_validation',
        python_callable=load_erp_sales_data,
        doc_md="Load ERP sales data with comprehensive validation"
    )

    load_crm_customers = PythonOperator(
        task_id='load_crm_customers_with_validation',
        python_callable=load_crm_customers_data,
        doc_md="Load CRM customers data with comprehensive validation"
    )
    
    # Final validation
    validate_bronze = PythonOperator(
        task_id='validate_bronze_layer_quality',
        python_callable=validate_bronze_layer,
        doc_md="Perform final bronze layer data quality validation"
    )

    # Set dependencies
    init_schemas >> create_bronze_tables >> [load_erp_sales, load_crm_customers] >> validate_bronze