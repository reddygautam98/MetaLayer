"""
MetaLayer Database Partitioning Setup DAG
=========================================

This DAG sets up and manages time-based table partitioning for optimal performance
and automated maintenance of the medallion architecture.

Features:
- Automated partition creation for Bronze, Silver, and Gold layers
- Time-based partitioning (monthly for orders, yearly for customers)
- Partition maintenance and cleanup procedures
- Performance optimization with proper indexing
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

def validate_partitioning_setup():
    """Validate that partitioning is properly configured"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Check if partition functions exist
        function_check = hook.get_first("""
            SELECT COUNT(*) 
            FROM pg_proc p 
            JOIN pg_namespace n ON p.pronamespace = n.oid 
            WHERE n.nspname = 'public' 
            AND p.proname IN ('create_monthly_partition', 'create_yearly_partition', 'auto_create_partitions')
        """)
        
        if function_check[0] < 3:
            raise AirflowException("Partitioning functions not found. Run partition setup first.")
        
        # Check partition tables exist
        partition_check = hook.get_first("""
            SELECT COUNT(DISTINCT schemaname||'.'||tablename) as partition_count
            FROM pg_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            AND (tablename LIKE '%_____' OR tablename LIKE '%______')
            AND tablename ~ '_\d{4}(_\d{2})?$'
        """)
        
        logger.info(f"Found {partition_check[0]} partitioned tables")
        
        # Validate partition structure
        structure_check = hook.get_records("""
            SELECT 
                pt.schemaname,
                pt.tablename,
                pg_size_pretty(pg_total_relation_size(pt.schemaname||'.'||pt.tablename)) as size,
                CASE 
                    WHEN pt.tablename ~ '_\d{4}_\d{2}$' THEN 'Monthly'
                    WHEN pt.tablename ~ '_\d{4}$' THEN 'Yearly'
                    ELSE 'Unknown'
                END as partition_type
            FROM pg_tables pt
            WHERE pt.schemaname IN ('bronze', 'silver', 'gold')
            AND pt.tablename ~ '_\d{4}(_\d{2})?$'
            ORDER BY pt.schemaname, pt.tablename
        """)
        
        validation_results = {
            'partition_functions': function_check[0],
            'partition_tables': partition_check[0],
            'partition_details': structure_check
        }
        
        logger.info(f"Partitioning validation completed: {validation_results}")
        return validation_results
        
    except Exception as e:
        logger.error(f"Partitioning validation failed: {e}")
        raise AirflowException(f"Validation failed: {e}")

def create_future_partitions():
    """Create partitions for upcoming months/years"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Create partitions for next 6 months
        result = hook.get_first("SELECT auto_create_partitions()")
        
        # Get partition statistics
        stats = hook.get_records("""
            SELECT 
                schemaname,
                COUNT(*) as partition_count,
                MIN(tablename) as earliest_partition,
                MAX(tablename) as latest_partition
            FROM pg_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            AND tablename ~ '_\d{4}(_\d{2})?$'
            GROUP BY schemaname
            ORDER BY schemaname
        """)
        
        logger.info(f"Future partitions created successfully")
        for schema_stat in stats:
            logger.info(f"Schema {schema_stat[0]}: {schema_stat[1]} partitions "
                       f"from {schema_stat[2]} to {schema_stat[3]}")
        
        return {
            'status': 'success',
            'partition_stats': stats
        }
        
    except Exception as e:
        logger.error(f"Failed to create future partitions: {e}")
        raise AirflowException(f"Partition creation failed: {e}")

def cleanup_old_partitions_task():
    """Clean up partitions older than retention period"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Cleanup partitions older than 24 months
        result = hook.get_first("SELECT cleanup_old_partitions(24)")
        dropped_count = result[0] if result else 0
        
        logger.info(f"Cleanup completed: {dropped_count} old partitions dropped")
        
        return {
            'status': 'success',
            'partitions_dropped': dropped_count
        }
        
    except Exception as e:
        logger.error(f"Partition cleanup failed: {e}")
        raise AirflowException(f"Cleanup failed: {e}")

def analyze_partition_performance():
    """Analyze partition performance and generate recommendations"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Get partition size analysis
        size_analysis = hook.get_records("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            AND tablename ~ '_\d{4}(_\d{2})?$'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            LIMIT 10
        """)
        
        # Check for partition pruning effectiveness
        pruning_analysis = hook.get_records("""
            SELECT 
                schemaname,
                COUNT(*) as total_partitions,
                AVG(pg_total_relation_size(schemaname||'.'||tablename)) as avg_partition_size,
                pg_size_pretty(AVG(pg_total_relation_size(schemaname||'.'||tablename))::bigint) as avg_size_formatted
            FROM pg_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            AND tablename ~ '_\d{4}(_\d{2})?$'
            GROUP BY schemaname
            ORDER BY schemaname
        """)
        
        performance_metrics = {
            'largest_partitions': size_analysis,
            'schema_statistics': pruning_analysis
        }
        
        # Generate recommendations
        recommendations = []
        for stat in pruning_analysis:
            schema_name = stat[0]
            partition_count = stat[1]
            avg_size = stat[2]
            
            if partition_count > 50:
                recommendations.append(f"Consider partition pruning for {schema_name} ({partition_count} partitions)")
            
            if avg_size > 1073741824:  # 1GB
                recommendations.append(f"Large partition sizes in {schema_name} - consider more granular partitioning")
        
        logger.info(f"Partition performance analysis completed")
        logger.info(f"Recommendations: {recommendations}")
        
        return {
            'performance_metrics': performance_metrics,
            'recommendations': recommendations
        }
        
    except Exception as e:
        logger.error(f"Performance analysis failed: {e}")
        raise AirflowException(f"Analysis failed: {e}")

# DAG Definition
with DAG(
    dag_id='database_partitioning_management',
    default_args=default_args,
    description='Automated database partitioning setup and maintenance for MetaLayer',
    schedule_interval='0 2 1 * *',  # Monthly at 2 AM on 1st day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['database', 'partitioning', 'maintenance', 'performance'],
    doc_md=__doc__
):
    
    # Task 1: Setup partitioned table structure
    setup_partitioning = SQLExecuteQueryOperator(
        task_id='setup_partitioning_structure',
        conn_id='postgres_default',
        sql='/opt/airflow/include/sql/partitioning_setup_pg.sql',
        doc_md="""
        Creates the partitioned table structure with:
        - Time-based partitions (monthly for orders, yearly for customers)
        - Proper indexing strategy for performance
        - Automated partition management functions
        """
    )
    
    # Task 2: Validate partitioning setup
    validate_setup = PythonOperator(
        task_id='validate_partitioning_setup',
        python_callable=validate_partitioning_setup,
        doc_md="Validates that all partitioning components are properly configured"
    )
    
    # Task 3: Create future partitions
    create_partitions = PythonOperator(
        task_id='create_future_partitions',
        python_callable=create_future_partitions,
        doc_md="Creates partitions for upcoming time periods to ensure continuous operation"
    )
    
    # Task 4: Cleanup old partitions
    cleanup_partitions = PythonOperator(
        task_id='cleanup_old_partitions',
        python_callable=cleanup_old_partitions_task,
        doc_md="Removes old partitions based on retention policy to manage storage"
    )
    
    # Task 5: Performance analysis
    analyze_performance = PythonOperator(
        task_id='analyze_partition_performance',
        python_callable=analyze_partition_performance,
        doc_md="Analyzes partition performance and generates optimization recommendations"
    )
    
    # Task dependencies
    setup_partitioning >> validate_setup >> create_partitions
    create_partitions >> [cleanup_partitions, analyze_performance]