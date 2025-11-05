"""
MetaLayer Data Quality Monitoring DAG
====================================

Comprehensive real-time data quality monitoring with automated checks,
alerting, and dashboard generation for the medallion architecture.

Features:
- Automated expectation suite generation
- Real-time quality validation
- Quality trend analysis and alerting
- Integration with monitoring dashboards
- Automated quality reports
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowException

# Import our data quality utilities
import sys
sys.path.append('/opt/airflow/include/utils')

try:
    # Use importlib to avoid circular import
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dq_utils", 
        "/opt/airflow/include/utils/data_quality_monitoring.py"
    )
    dq_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dq_utils)
    
    # Access the functions from the loaded module
    create_quality_monitor = dq_utils.create_quality_monitor
    run_table_quality_check = dq_utils.run_table_quality_check
    get_quality_dashboard = dq_utils.get_quality_dashboard
    
    DQ_AVAILABLE = True
except Exception as e:
    logging.error(f"Data quality monitoring utilities not available: {e}")
    DQ_AVAILABLE = False

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

def initialize_quality_monitoring():
    """Initialize data quality monitoring infrastructure"""
    if not DQ_AVAILABLE:
        raise AirflowException("Data quality monitoring not available")
    
    try:
        monitor = create_quality_monitor('postgres_default')
        logger.info("Data quality monitoring initialized successfully")
        
        return {
            'status': 'success',
            'message': 'Quality monitoring infrastructure ready',
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to initialize quality monitoring: {e}")
        raise AirflowException(f"Quality monitoring initialization failed: {e}")

def setup_bronze_quality_suites():
    """Set up expectation suites for bronze layer tables"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping suite setup")
        return {'status': 'skipped'}
    
    try:
        monitor = create_quality_monitor('postgres_default')
        
        # Define bronze layer tables to monitor
        bronze_tables = [
            ('crm_customers_raw', 'bronze'),
            ('erp_orders_raw', 'bronze')
        ]
        
        results = {}
        
        for table_name, schema_name in bronze_tables:
            try:
                suite_info = monitor.create_expectation_suite(table_name, schema_name)
                results[f"{schema_name}.{table_name}"] = suite_info
                logger.info(f"Created expectation suite for {schema_name}.{table_name}")
                
            except Exception as e:
                logger.error(f"Failed to create suite for {table_name}: {e}")
                results[f"{schema_name}.{table_name}"] = {'error': str(e)}
        
        return {
            'status': 'completed',
            'suites_created': len([r for r in results.values() if 'error' not in r]),
            'suites_failed': len([r for r in results.values() if 'error' in r]),
            'details': results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Bronze suite setup failed: {e}")
        raise AirflowException(f"Suite setup failed: {e}")

def run_bronze_quality_checks():
    """Run quality checks for all bronze layer tables"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping quality checks")
        return {'status': 'skipped'}
    
    try:
        # Tables to check
        tables_to_check = [
            ('crm_customers_raw', 'bronze'),
            ('erp_orders_raw', 'bronze')
        ]
        
        results = {}
        total_success_rate = 0
        check_count = 0
        
        for table_name, schema_name in tables_to_check:
            try:
                result = run_table_quality_check(table_name, schema_name, 'postgres_default')
                results[f"{schema_name}.{table_name}"] = result
                
                success_rate = result.get('success_rate', 0)
                total_success_rate += success_rate
                check_count += 1
                
                logger.info(f"Quality check for {schema_name}.{table_name}: {success_rate}%")
                
                # Alert on low quality
                if success_rate < 95:
                    logger.warning(f"Low data quality detected for {schema_name}.{table_name}: {success_rate}%")
                
            except Exception as e:
                logger.error(f"Quality check failed for {table_name}: {e}")
                results[f"{schema_name}.{table_name}"] = {'error': str(e)}
        
        # Calculate overall quality score
        overall_quality = (total_success_rate / check_count) if check_count > 0 else 0
        
        return {
            'status': 'completed',
            'overall_quality_score': round(overall_quality, 2),
            'checks_completed': check_count,
            'checks_failed': len([r for r in results.values() if 'error' in r]),
            'tables_below_threshold': len([r for r in results.values() if isinstance(r, dict) and r.get('success_rate', 100) < 95]),
            'detailed_results': results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Bronze quality checks failed: {e}")
        raise AirflowException(f"Quality checks failed: {e}")

def run_silver_quality_checks():
    """Run quality checks for silver layer tables"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping silver checks")
        return {'status': 'skipped'}
    
    try:
        # Silver layer tables
        tables_to_check = [
            ('orders', 'silver'),
            ('customers', 'silver')
        ]
        
        results = {}
        
        for table_name, schema_name in tables_to_check:
            try:
                # First ensure expectation suite exists
                monitor = create_quality_monitor('postgres_default')
                monitor.create_expectation_suite(table_name, schema_name)
                
                # Run quality check
                result = run_table_quality_check(table_name, schema_name, 'postgres_default')
                results[f"{schema_name}.{table_name}"] = result
                
                success_rate = result.get('success_rate', 0)
                logger.info(f"Silver quality check for {schema_name}.{table_name}: {success_rate}%")
                
            except Exception as e:
                logger.error(f"Silver quality check failed for {table_name}: {e}")
                results[f"{schema_name}.{table_name}"] = {'error': str(e)}
        
        return {
            'status': 'completed',
            'silver_results': results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Silver quality checks failed: {e}")
        raise AirflowException(f"Silver quality checks failed: {e}")

def run_gold_quality_checks():
    """Run quality checks for gold layer tables"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping gold checks")
        return {'status': 'skipped'}
    
    try:
        # Gold layer tables  
        tables_to_check = [
            ('dim_customer', 'gold'),
            ('fact_sales', 'gold')
        ]
        
        results = {}
        
        for table_name, schema_name in tables_to_check:
            try:
                # Ensure expectation suite exists
                monitor = create_quality_monitor('postgres_default')
                monitor.create_expectation_suite(table_name, schema_name)
                
                # Run quality check
                result = run_table_quality_check(table_name, schema_name, 'postgres_default')
                results[f"{schema_name}.{table_name}"] = result
                
                success_rate = result.get('success_rate', 0)
                logger.info(f"Gold quality check for {schema_name}.{table_name}: {success_rate}%")
                
            except Exception as e:
                logger.error(f"Gold quality check failed for {table_name}: {e}")
                results[f"{schema_name}.{table_name}"] = {'error': str(e)}
        
        return {
            'status': 'completed',
            'gold_results': results,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Gold quality checks failed: {e}")
        raise AirflowException(f"Gold quality checks failed: {e}")

def generate_quality_dashboard():
    """Generate comprehensive data quality dashboard"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping dashboard")
        return {'status': 'skipped'}
    
    try:
        dashboard_data = get_quality_dashboard()
        
        # Log summary information
        summary = dashboard_data.get('summary', {})
        logger.info(f"Quality Dashboard Summary:")
        logger.info(f"  - Total Tables: {summary.get('total_tables', 0)}")
        logger.info(f"  - Average Quality Score: {summary.get('avg_quality_score', 0):.2f}%")
        logger.info(f"  - Tables Below Threshold: {summary.get('tables_below_threshold', 0)}")
        
        # Alert on overall quality issues
        avg_quality = summary.get('avg_quality_score', 100)
        if avg_quality < 95:
            logger.warning(f"Overall data quality below threshold: {avg_quality:.2f}%")
        
        return {
            'status': 'completed',
            'dashboard_data': dashboard_data,
            'quality_summary': summary,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Dashboard generation failed: {e}")
        raise AirflowException(f"Dashboard generation failed: {e}")

def analyze_quality_trends():
    """Analyze data quality trends and generate recommendations"""
    if not DQ_AVAILABLE:
        logger.warning("Data quality monitoring not available - skipping trend analysis")
        return {'status': 'skipped'}
    
    try:
        dashboard_data = get_quality_dashboard()
        trends = dashboard_data.get('quality_trends', [])
        
        if len(trends) < 2:
            logger.info("Insufficient data for trend analysis")
            return {'status': 'insufficient_data'}
        
        # Analyze trends
        recent_quality = trends[0]['avg_success_rate'] if trends else 0
        previous_quality = trends[1]['avg_success_rate'] if len(trends) > 1 else recent_quality
        
        quality_change = recent_quality - previous_quality
        
        # Generate recommendations
        recommendations = []
        
        if quality_change < -5:
            recommendations.append("Data quality declining - investigate recent changes")
        elif quality_change > 5:
            recommendations.append("Data quality improving - continue current practices")
        
        failed_metrics = dashboard_data.get('failed_metrics', [])
        if failed_metrics:
            top_failing = failed_metrics[0]
            recommendations.append(f"Focus on {top_failing['metric_name']} for {top_failing['table_name']}")
        
        if recent_quality < 90:
            recommendations.append("Critical: Overall quality below 90% - immediate attention required")
        
        trend_analysis = {
            'status': 'completed',
            'recent_quality': recent_quality,
            'quality_change': round(quality_change, 2),
            'trend_direction': 'improving' if quality_change > 0 else 'declining' if quality_change < 0 else 'stable',
            'recommendations': recommendations,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Quality trend analysis: {trend_analysis}")
        return trend_analysis
        
    except Exception as e:
        logger.error(f"Trend analysis failed: {e}")
        raise AirflowException(f"Trend analysis failed: {e}")

# DAG Definition
with DAG(
    dag_id='data_quality_monitoring',
    default_args=default_args,
    description='Comprehensive real-time data quality monitoring for MetaLayer medallion architecture',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['data-quality', 'monitoring', 'great-expectations', 'medallion'],
    doc_md=__doc__
):
    
    # Task 1: Initialize monitoring infrastructure
    init_monitoring = PythonOperator(
        task_id='initialize_quality_monitoring',
        python_callable=initialize_quality_monitoring,
        doc_md="Initialize data quality monitoring infrastructure and tables"
    )
    
    # Task 2: Setup expectation suites for bronze layer
    setup_bronze_suites = PythonOperator(
        task_id='setup_bronze_quality_suites',
        python_callable=setup_bronze_quality_suites,
        doc_md="Create and configure Great Expectations suites for bronze layer tables"
    )
    
    # Task 3: Run bronze layer quality checks
    check_bronze_quality = PythonOperator(
        task_id='run_bronze_quality_checks',
        python_callable=run_bronze_quality_checks,
        doc_md="Execute comprehensive quality validation for bronze layer data"
    )
    
    # Task 4: Run silver layer quality checks
    check_silver_quality = PythonOperator(
        task_id='run_silver_quality_checks',
        python_callable=run_silver_quality_checks,
        doc_md="Execute quality validation for silver layer transformed data"
    )
    
    # Task 5: Run gold layer quality checks
    check_gold_quality = PythonOperator(
        task_id='run_gold_quality_checks',
        python_callable=run_gold_quality_checks,
        doc_md="Execute quality validation for gold layer analytical data"
    )
    
    # Task 6: Generate quality dashboard
    generate_dashboard = PythonOperator(
        task_id='generate_quality_dashboard',
        python_callable=generate_quality_dashboard,
        doc_md="Generate comprehensive data quality dashboard and metrics"
    )
    
    # Task 7: Analyze quality trends
    analyze_trends = PythonOperator(
        task_id='analyze_quality_trends',
        python_callable=analyze_quality_trends,
        doc_md="Analyze quality trends and generate actionable recommendations"
    )
    
    # Task dependencies
    init_monitoring >> setup_bronze_suites >> check_bronze_quality
    check_bronze_quality >> [check_silver_quality, check_gold_quality]
    [check_silver_quality, check_gold_quality] >> generate_dashboard >> analyze_trends