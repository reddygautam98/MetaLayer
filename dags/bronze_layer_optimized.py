"""
MetaLayer Optimized Bronze Layer DAG
===================================

Enhanced bronze layer data ingestion with:
- Incremental processing for large datasets
- Optimized connection pooling
- Automated partitioning support
- Performance monitoring and alerting

This DAG replaces the basic bronze layer implementation with enterprise-grade
data processing capabilities.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException

# Import our custom utilities
import sys
import os
sys.path.append('/opt/airflow/include/utils')

from incremental_processing import IncrementalProcessor, IncrementalStrategy
from connection_pooling import get_optimized_connection

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=4),
    'email': ['data-team@yourdomain.com']
}

# Configuration
DATA_DIR = '/opt/airflow/data/bronze_src'
CHUNK_SIZE = 50000

def validate_data_sources():
    """Validate that all required data sources are available and accessible"""
    required_files = [
        f"{DATA_DIR}/crm/customers.csv",
        f"{DATA_DIR}/erp/orders.csv"
    ]
    
    validation_results = {}
    
    for file_path in required_files:
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data source not found: {file_path}")
            
            file_size = os.path.getsize(file_path)
            validation_results[file_path] = {
                'exists': True,
                'size_bytes': file_size,
                'size_mb': round(file_size / 1024 / 1024, 2),
                'last_modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
            }
            
        except Exception as e:
            validation_results[file_path] = {
                'exists': False,
                'error': str(e)
            }
    
    # Check if any files are missing
    missing_files = [f for f, info in validation_results.items() if not info.get('exists', False)]
    if missing_files:
        raise AirflowException(f"Missing required data sources: {missing_files}")
    
    logger.info(f"Data source validation completed: {validation_results}")
    return validation_results

def load_customers_incremental():
    """Load customer data using incremental processing"""
    try:
        # Initialize incremental processor
        processor = IncrementalProcessor(conn_id='postgres_default', chunk_size=CHUNK_SIZE)
        
        # Process customer data with timestamp-based incremental loading
        result = processor.process_csv_incremental_timestamp(
            csv_path=f"{DATA_DIR}/crm/customers.csv",
            target_table="crm_customers_raw",
            target_schema="bronze",
            timestamp_column="created_date"
        )
        
        logger.info(f"Customer incremental loading completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Customer incremental loading failed: {e}")
        raise AirflowException(f"Customer loading failed: {e}")

def load_orders_incremental():
    """Load order data using incremental processing"""
    try:
        # Initialize incremental processor
        processor = IncrementalProcessor(conn_id='postgres_default', chunk_size=CHUNK_SIZE)
        
        # Process order data with timestamp-based incremental loading
        result = processor.process_csv_incremental_timestamp(
            csv_path=f"{DATA_DIR}/erp/orders.csv",
            target_table="erp_orders_raw", 
            target_schema="bronze",
            timestamp_column="order_date"
        )
        
        logger.info(f"Order incremental loading completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Order incremental loading failed: {e}")
        raise AirflowException(f"Order loading failed: {e}")

def validate_bronze_data_quality():
    """Comprehensive data quality validation for bronze layer"""
    try:
        # Get optimized connection pool
        connection_pool = get_optimized_connection('postgres_default')
        
        # Data quality checks
        quality_checks = {}
        
        # Check 1: Record counts
        customer_count = connection_pool.execute_query(
            "SELECT COUNT(*) FROM bronze.crm_customers_raw"
        )[0]
        
        order_count = connection_pool.execute_query(
            "SELECT COUNT(*) FROM bronze.erp_orders_raw"
        )[0]
        
        quality_checks['record_counts'] = {
            'customers': customer_count,
            'orders': order_count
        }
        
        # Check 2: Data freshness
        latest_customer = connection_pool.execute_query(
            "SELECT MAX(created_date) FROM bronze.crm_customers_raw"
        )[0]
        
        latest_order = connection_pool.execute_query(
            "SELECT MAX(order_date) FROM bronze.erp_orders_raw"
        )[0]
        
        quality_checks['data_freshness'] = {
            'latest_customer_date': latest_customer.isoformat() if latest_customer else None,
            'latest_order_date': latest_order.isoformat() if latest_order else None
        }
        
        # Check 3: Data completeness
        null_customers = connection_pool.execute_query(
            "SELECT COUNT(*) FROM bronze.crm_customers_raw WHERE customer_id IS NULL OR email IS NULL"
        )[0]
        
        null_orders = connection_pool.execute_query(
            "SELECT COUNT(*) FROM bronze.erp_orders_raw WHERE order_id IS NULL OR customer_id IS NULL"
        )[0]
        
        quality_checks['data_completeness'] = {
            'customers_with_nulls': null_customers,
            'orders_with_nulls': null_orders,
            'customer_completeness_rate': round((1 - null_customers/customer_count) * 100, 2) if customer_count > 0 else 0,
            'order_completeness_rate': round((1 - null_orders/order_count) * 100, 2) if order_count > 0 else 0
        }
        
        # Check 4: Referential integrity
        orphaned_orders = connection_pool.execute_query("""
            SELECT COUNT(*) 
            FROM bronze.erp_orders_raw o 
            LEFT JOIN bronze.crm_customers_raw c ON o.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL
        """)[0]
        
        quality_checks['referential_integrity'] = {
            'orphaned_orders': orphaned_orders,
            'integrity_rate': round((1 - orphaned_orders/order_count) * 100, 2) if order_count > 0 else 100
        }
        
        # Check 5: Business rule validation
        invalid_orders = connection_pool.execute_query("""
            SELECT COUNT(*) 
            FROM bronze.erp_orders_raw 
            WHERE quantity <= 0 OR price_per_unit <= 0
        """)[0]
        
        quality_checks['business_rules'] = {
            'invalid_orders': invalid_orders,
            'business_rule_compliance': round((1 - invalid_orders/order_count) * 100, 2) if order_count > 0 else 100
        }
        
        # Overall quality score
        completeness_avg = (quality_checks['data_completeness']['customer_completeness_rate'] + 
                          quality_checks['data_completeness']['order_completeness_rate']) / 2
        
        overall_score = (
            completeness_avg + 
            quality_checks['referential_integrity']['integrity_rate'] + 
            quality_checks['business_rules']['business_rule_compliance']
        ) / 3
        
        quality_checks['overall_quality_score'] = round(overall_score, 2)
        
        # Determine if quality is acceptable
        if overall_score < 95:
            logger.warning(f"Data quality below threshold: {overall_score}%")
        
        logger.info(f"Bronze data quality validation completed: {quality_checks}")
        return quality_checks
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {e}")
        raise AirflowException(f"Quality validation failed: {e}")

def monitor_connection_performance():
    """Monitor and log connection pool performance"""
    try:
        from connection_pooling import ConnectionPoolManager
        
        # Get status of all connection pools
        pool_status = ConnectionPoolManager.get_all_pool_status()
        
        # Log performance metrics
        for conn_id, status in pool_status.items():
            if 'error' not in status:
                logger.info(f"Connection pool {conn_id} status: "
                          f"Active: {status.get('active_connections', 0)}, "
                          f"Utilization: {status.get('pool_utilization', 0)}%, "
                          f"Avg Wait: {status.get('average_wait_time', 0)}s")
                
                # Alert on high utilization
                if status.get('pool_utilization', 0) > 80:
                    logger.warning(f"High connection pool utilization for {conn_id}: "
                                 f"{status.get('pool_utilization')}%")
            else:
                logger.error(f"Connection pool {conn_id} error: {status['error']}")
        
        return pool_status
        
    except Exception as e:
        logger.error(f"Connection performance monitoring failed: {e}")
        return {}

def generate_bronze_report():
    """Generate comprehensive bronze layer processing report"""
    try:
        # Get incremental processing statistics
        processor = IncrementalProcessor(conn_id='postgres_default')
        incremental_stats = processor.get_incremental_statistics()
        
        # Get connection pool performance
        pool_performance = monitor_connection_performance()
        
        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'incremental_processing': incremental_stats,
            'connection_performance': pool_performance,
            'data_validation': validate_bronze_data_quality(),
            'processing_summary': {
                'total_tables_processed': incremental_stats.get('total_tables', 0),
                'successful_runs': incremental_stats.get('successful_runs', 0),
                'failed_runs': incremental_stats.get('failed_runs', 0),
                'success_rate': round(
                    (incremental_stats.get('successful_runs', 0) / 
                     max(incremental_stats.get('total_tables', 1), 1)) * 100, 2
                )
            }
        }
        
        logger.info(f"Bronze layer processing report: {report}")
        return report
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return {'error': str(e)}

# DAG Definition
with DAG(
    dag_id='bronze_layer_optimized',
    default_args=default_args,
    description='Optimized bronze layer data ingestion with incremental processing and connection pooling',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'incremental', 'optimized', 'data-ingestion'],
    doc_md=__doc__
):
    
    # Task 1: Validate data sources
    validate_sources = PythonOperator(
        task_id='validate_data_sources',
        python_callable=validate_data_sources,
        doc_md="Validates availability and accessibility of all required data sources"
    )
    
    # Task 2: Setup bronze tables (run once or when schema changes)
    setup_bronze_tables = SQLExecuteQueryOperator(
        task_id='setup_bronze_tables',
        conn_id='postgres_default',
        sql='/opt/airflow/include/sql/bronze_ddl_pg.sql',
        doc_md="Creates bronze layer table structures with partitioning if not exists"
    )
    
    # Task 3: Load customers incrementally
    load_customers = PythonOperator(
        task_id='load_customers_incremental',
        python_callable=load_customers_incremental,
        doc_md="Loads customer data using timestamp-based incremental processing"
    )
    
    # Task 4: Load orders incrementally  
    load_orders = PythonOperator(
        task_id='load_orders_incremental',
        python_callable=load_orders_incremental,
        doc_md="Loads order data using timestamp-based incremental processing"
    )
    
    # Task 5: Validate data quality
    validate_quality = PythonOperator(
        task_id='validate_bronze_data_quality',
        python_callable=validate_bronze_data_quality,
        doc_md="Comprehensive data quality validation for bronze layer"
    )
    
    # Task 6: Monitor performance
    monitor_performance = PythonOperator(
        task_id='monitor_connection_performance', 
        python_callable=monitor_connection_performance,
        doc_md="Monitor connection pool performance and generate alerts"
    )
    
    # Task 7: Generate processing report
    generate_report = PythonOperator(
        task_id='generate_bronze_report',
        python_callable=generate_bronze_report,
        doc_md="Generate comprehensive processing report with metrics and insights"
    )
    
    # Task dependencies
    validate_sources >> setup_bronze_tables
    setup_bronze_tables >> [load_customers, load_orders]
    [load_customers, load_orders] >> validate_quality
    validate_quality >> monitor_performance >> generate_report