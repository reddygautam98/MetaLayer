"""
MASTER ORCHESTRATOR DAG - ETL PIPELINE COORDINATION
==================================================

This DAG orchestrates the complete ETL pipeline across Bronze, Silver, and Gold layers:
- Coordinates the entire medallion architecture pipeline
- Manages dependencies and error handling across layers
- Implements comprehensive monitoring and alerting
- Provides centralized pipeline status and metrics
- Handles data lineage and audit logging

Features:
- Docker-optimized orchestration
- Cross-layer dependency management
- Comprehensive error handling and recovery
- Real-time monitoring and alerting
- Performance tracking and optimization
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

# Add include path for utilities
import sys
sys.path.append('/opt/airflow/include')
from utils.metrics_exporter import export_pipeline_metrics

# =====================================================
# CONFIGURATION & CONSTANTS
# =====================================================
DAG_ID = 'master_etl_orchestrator'
PIPELINE_VERSION = Variable.get("pipeline_version", default_var="1.0.0")
SLA_MINUTES = int(Variable.get("pipeline_sla_minutes", default_var=240))  # 4 hours

# Setup logging
logger = logging.getLogger(__name__)

# =====================================================
# DAG DEFAULT ARGUMENTS
# =====================================================
default_args = {
    'owner': 'data-platform-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,  # No retries for orchestrator
    'execution_timeout': timedelta(minutes=SLA_MINUTES),
    'email': [Variable.get("alert_email", default_var="data-platform@company.com")],
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    'sla': timedelta(minutes=SLA_MINUTES)
}

# =====================================================
# ORCHESTRATION FUNCTIONS
# =====================================================
def initialize_pipeline_run(**context) -> Dict[str, Any]:
    """Initialize pipeline run with metadata and validation"""
    execution_date = context['execution_date']
    
    logger.info(f"🚀 Initializing ETL pipeline run for {execution_date}")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        pipeline_metadata = {
            'execution_date': execution_date.isoformat(),
            'pipeline_version': PIPELINE_VERSION,
            'start_time': datetime.now().isoformat(),
            'status': 'INITIALIZING',
            'layers_completed': [],
            'total_records_processed': 0,
            'data_quality_score': 0.0,
            'sla_minutes': SLA_MINUTES
        }
        
        # Check system prerequisites
        prerequisites = check_system_prerequisites(hook, execution_date)
        pipeline_metadata['prerequisites'] = prerequisites
        
        if not prerequisites['all_passed']:
            raise AirflowException(f"Prerequisites failed: {prerequisites['failed_checks']}")
        
        # Initialize pipeline run in database
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            
            insert_query = """
                INSERT INTO airflow_meta.pipeline_runs (
                    dag_id, execution_date, pipeline_version, status, 
                    metadata, start_time
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_id, execution_date) DO UPDATE SET
                    pipeline_version = EXCLUDED.pipeline_version,
                    status = EXCLUDED.status,
                    metadata = EXCLUDED.metadata,
                    start_time = EXCLUDED.start_time
            """
            
            cursor.execute(insert_query, [
                DAG_ID, execution_date, PIPELINE_VERSION, 'INITIALIZING',
                json.dumps(pipeline_metadata), datetime.now()
            ])
            
            conn.commit()
        
        logger.info(f"✅ Pipeline initialized successfully: {pipeline_metadata}")
        
        # Export initialization metrics
        init_metrics = {
            'prerequisites_passed': len(prerequisites['passed_checks']),
            'prerequisites_failed': len(prerequisites['failed_checks']),
            'initialization_time': (datetime.now() - datetime.fromisoformat(pipeline_metadata['start_time'])).total_seconds()
        }
        
        export_pipeline_metrics('master_initialization', init_metrics, execution_date)
        
        return pipeline_metadata
        
    except Exception as e:
        logger.error(f"❌ Pipeline initialization failed: {str(e)}")
        raise

def check_system_prerequisites(hook: PostgresHook, execution_date: datetime) -> Dict[str, Any]:
    """Check system prerequisites before pipeline execution"""
    
    logger.info("🔍 Checking system prerequisites")
    
    prerequisites = {
        'all_passed': True,
        'passed_checks': [],
        'failed_checks': [],
        'warnings': []
    }
    
    checks = [
        {
            'name': 'database_connectivity',
            'query': 'SELECT 1',
            'expected_result': 1,
            'critical': True
        },
        {
            'name': 'bronze_schema_exists',
            'query': "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'bronze'",
            'expected_result': 1,
            'critical': True
        },
        {
            'name': 'silver_schema_exists',
            'query': "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'silver'",
            'expected_result': 1,
            'critical': True
        },
        {
            'name': 'gold_schema_exists',
            'query': "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'gold'",
            'expected_result': 1,
            'critical': True
        },
        {
            'name': 'source_data_availability',
            'query': f"SELECT COUNT(*) FROM bronze.customers_raw WHERE DATE(created_date) >= '{execution_date.date()}'",
            'expected_min': 0,  # Allow zero for new days
            'critical': False
        },
        {
            'name': 'disk_space_check',
            'query': "SELECT pg_size_pretty(pg_database_size(current_database()))",
            'expected_result': None,  # Just check it runs
            'critical': False
        }
    ]
    
    for check in checks:
        try:
            result = hook.get_first(check['query'])
            
            if check['name'] == 'disk_space_check':
                # Just log the result for monitoring
                logger.info(f"📊 Database size: {result[0] if result else 'Unknown'}")
                prerequisites['passed_checks'].append(check['name'])
                continue
            
            if result and len(result) > 0:
                actual_value = result[0]
                
                if 'expected_result' in check:
                    if actual_value == check['expected_result']:
                        prerequisites['passed_checks'].append(check['name'])
                    else:
                        if check['critical']:
                            prerequisites['failed_checks'].append({
                                'check': check['name'],
                                'expected': check['expected_result'],
                                'actual': actual_value
                            })
                            prerequisites['all_passed'] = False
                        else:
                            prerequisites['warnings'].append({
                                'check': check['name'],
                                'message': f"Expected {check['expected_result']}, got {actual_value}"
                            })
                
                elif 'expected_min' in check:
                    if actual_value >= check['expected_min']:
                        prerequisites['passed_checks'].append(check['name'])
                    else:
                        if check['critical']:
                            prerequisites['failed_checks'].append({
                                'check': check['name'],
                                'expected_min': check['expected_min'],
                                'actual': actual_value
                            })
                            prerequisites['all_passed'] = False
                        else:
                            prerequisites['warnings'].append({
                                'check': check['name'],
                                'message': f"Below minimum {check['expected_min']}, got {actual_value}"
                            })
            else:
                if check['critical']:
                    prerequisites['failed_checks'].append({
                        'check': check['name'],
                        'message': 'No result returned'
                    })
                    prerequisites['all_passed'] = False
        
        except Exception as e:
            if check['critical']:
                prerequisites['failed_checks'].append({
                    'check': check['name'],
                    'error': str(e)
                })
                prerequisites['all_passed'] = False
            else:
                prerequisites['warnings'].append({
                    'check': check['name'],
                    'error': str(e)
                })
    
    logger.info(f"✅ Prerequisites check completed: {len(prerequisites['passed_checks'])} passed, {len(prerequisites['failed_checks'])} failed")
    
    return prerequisites

def monitor_layer_completion(layer_name: str, **context) -> Dict[str, Any]:
    """Monitor and log completion of each layer"""
    execution_date = context['execution_date']
    
    logger.info(f"📋 Monitoring {layer_name} layer completion for {execution_date}")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get layer metrics
        layer_metrics = get_layer_metrics(hook, layer_name, execution_date)
        
        # Update pipeline run status
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            
            # Get current pipeline metadata
            select_query = """
                SELECT metadata FROM airflow_meta.pipeline_runs 
                WHERE dag_id = %s AND execution_date = %s
            """
            
            cursor.execute(select_query, [DAG_ID, execution_date])
            result = cursor.fetchone()
            
            if result:
                metadata = json.loads(result[0])
                metadata['layers_completed'].append({
                    'layer': layer_name,
                    'completion_time': datetime.now().isoformat(),
                    'metrics': layer_metrics
                })
                
                # Update metadata
                update_query = """
                    UPDATE airflow_meta.pipeline_runs 
                    SET metadata = %s, updated_time = %s
                    WHERE dag_id = %s AND execution_date = %s
                """
                
                cursor.execute(update_query, [
                    json.dumps(metadata), datetime.now(), DAG_ID, execution_date
                ])
                
                conn.commit()
        
        logger.info(f"✅ {layer_name} layer monitoring completed: {layer_metrics}")
        
        return layer_metrics
        
    except Exception as e:
        logger.error(f"❌ {layer_name} layer monitoring failed: {str(e)}")
        raise

def get_layer_metrics(hook: PostgresHook, layer_name: str, execution_date: datetime) -> Dict[str, Any]:
    """Get metrics for a specific layer"""
    
    metrics = {
        'records_processed': 0,
        'data_quality_score': 0.0,
        'processing_time_seconds': 0,
        'errors_count': 0
    }
    
    try:
        # Get metrics from layer-specific tables
        if layer_name == 'bronze':
            query = """
                SELECT 
                    COUNT(*) as records,
                    AVG(CASE WHEN validation_status = 'VALID' THEN 1.0 ELSE 0.0 END) as quality_score
                FROM bronze.customers_raw 
                WHERE DATE(ingestion_timestamp) = %s
                
                UNION ALL
                
                SELECT 
                    COUNT(*) as records,
                    AVG(CASE WHEN validation_status = 'VALID' THEN 1.0 ELSE 0.0 END) as quality_score
                FROM bronze.orders_raw 
                WHERE DATE(ingestion_timestamp) = %s
            """
            
            results = hook.get_records(query, parameters=[execution_date.date(), execution_date.date()])
            
            if results:
                total_records = sum(row[0] for row in results)
                avg_quality = sum(row[1] for row in results if row[1] is not None) / len([r for r in results if r[1] is not None]) if results else 0
                
                metrics['records_processed'] = total_records
                metrics['data_quality_score'] = avg_quality
        
        elif layer_name == 'silver':
            query = """
                SELECT 
                    COUNT(*) as records,
                    AVG(data_quality_score) as quality_score
                FROM silver.customers_cleaned 
                WHERE DATE(processed_timestamp) = %s
                
                UNION ALL
                
                SELECT 
                    COUNT(*) as records,
                    AVG(CASE WHEN is_valid THEN 1.0 ELSE 0.0 END) as quality_score
                FROM silver.orders_cleaned 
                WHERE DATE(processed_timestamp) = %s
            """
            
            results = hook.get_records(query, parameters=[execution_date.date(), execution_date.date()])
            
            if results:
                total_records = sum(row[0] for row in results)
                avg_quality = sum(row[1] for row in results if row[1] is not None) / len([r for r in results if r[1] is not None]) if results else 0
                
                metrics['records_processed'] = total_records
                metrics['data_quality_score'] = avg_quality
        
        elif layer_name == 'gold':
            query = f"""
                SELECT 
                    total_orders,
                    1.0 as quality_score  -- Assume high quality for gold layer
                FROM gold.agg_daily_sales 
                WHERE date_key = {int(execution_date.strftime('%Y%m%d'))}
            """
            
            result = hook.get_first(query)
            
            if result:
                metrics['records_processed'] = result[0] or 0
                metrics['data_quality_score'] = result[1] or 0.0
    
    except Exception as e:
        logger.warning(f"⚠️ Could not get metrics for {layer_name} layer: {str(e)}")
    
    return metrics

def finalize_pipeline_run(**context) -> Dict[str, Any]:
    """Finalize pipeline run with summary and cleanup"""
    execution_date = context['execution_date']
    
    logger.info(f"🏁 Finalizing ETL pipeline run for {execution_date}")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get final pipeline status
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            
            # Get pipeline metadata
            select_query = """
                SELECT metadata, start_time FROM airflow_meta.pipeline_runs 
                WHERE dag_id = %s AND execution_date = %s
            """
            
            cursor.execute(select_query, [DAG_ID, execution_date])
            result = cursor.fetchone()
            
            if not result:
                raise AirflowException("Pipeline run metadata not found")
            
            metadata = json.loads(result[0])
            start_time = result[1]
            
            # Calculate final metrics
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds()
            
            final_summary = {
                'execution_date': execution_date.isoformat(),
                'total_duration_seconds': total_duration,
                'layers_completed': len(metadata.get('layers_completed', [])),
                'total_records_processed': sum(
                    layer['metrics']['records_processed'] 
                    for layer in metadata.get('layers_completed', [])
                ),
                'overall_quality_score': (
                    sum(layer['metrics']['data_quality_score'] for layer in metadata.get('layers_completed', [])) /
                    len(metadata.get('layers_completed', []))
                    if metadata.get('layers_completed', []) else 0.0
                ),
                'sla_met': total_duration <= (SLA_MINUTES * 60),
                'status': 'COMPLETED'
            }
            
            # Update final status
            update_query = """
                UPDATE airflow_meta.pipeline_runs 
                SET status = %s, end_time = %s, 
                    total_duration_seconds = %s, total_records = %s,
                    quality_score = %s, sla_met = %s
                WHERE dag_id = %s AND execution_date = %s
            """
            
            cursor.execute(update_query, [
                final_summary['status'], end_time,
                final_summary['total_duration_seconds'], final_summary['total_records_processed'],
                final_summary['overall_quality_score'], final_summary['sla_met'],
                DAG_ID, execution_date
            ])
            
            conn.commit()
        
        logger.info(f"✅ Pipeline finalized successfully: {final_summary}")
        
        # Generate alerts if needed
        if not final_summary['sla_met']:
            logger.warning(f"⚠️ SLA missed: {total_duration/60:.1f} minutes (SLA: {SLA_MINUTES} minutes)")
        
        if final_summary['overall_quality_score'] < 0.8:
            logger.warning(f"⚠️ Quality score below threshold: {final_summary['overall_quality_score']:.2%}")
        
        # Export final metrics
        export_pipeline_metrics('master_finalization', final_summary, execution_date)
        
        return final_summary
        
    except Exception as e:
        logger.error(f"❌ Pipeline finalization failed: {str(e)}")
        # Update status to failed
        try:
            hook = PostgresHook(postgres_conn_id='postgres_default')
            with hook.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE airflow_meta.pipeline_runs SET status = %s, end_time = %s WHERE dag_id = %s AND execution_date = %s",
                    ['FAILED', datetime.now(), DAG_ID, execution_date]
                )
                conn.commit()
        except Exception as update_error:
            logger.error(f"❌ Failed to update status: {str(update_error)}")
        
        raise

# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Master ETL Orchestrator - Coordinates Bronze, Silver, and Gold Layer Processing',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    max_active_runs=1,
    concurrency=1,
    catchup=False,
    tags=['etl', 'orchestrator', 'master', 'coordination', 'production']
)

# =====================================================
# TASK DEFINITIONS
# =====================================================

# Initialize pipeline
initialize_pipeline_task = PythonOperator(
    task_id='initialize_pipeline',
    python_callable=initialize_pipeline_run,
    dag=dag,
    doc_md="""
    ## Initialize Pipeline
    
    Initializes the ETL pipeline run:
    - Validates system prerequisites
    - Creates pipeline run metadata
    - Performs health checks
    - Sets up monitoring and logging
    """
)

# Trigger Bronze layer
trigger_bronze_task = TriggerDagRunOperator(
    task_id='trigger_bronze_layer',
    trigger_dag_id='bronze_layer_etl_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=dag
)

# Monitor Bronze completion
monitor_bronze_task = PythonOperator(
    task_id='monitor_bronze_completion',
    python_callable=lambda **context: monitor_layer_completion('bronze', **context),
    dag=dag
)

# Trigger Silver layer
trigger_silver_task = TriggerDagRunOperator(
    task_id='trigger_silver_layer',
    trigger_dag_id='silver_layer_etl_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=dag
)

# Monitor Silver completion
monitor_silver_task = PythonOperator(
    task_id='monitor_silver_completion',
    python_callable=lambda **context: monitor_layer_completion('silver', **context),
    dag=dag
)

# Trigger Gold layer
trigger_gold_task = TriggerDagRunOperator(
    task_id='trigger_gold_layer',
    trigger_dag_id='gold_layer_analytics_pipeline',
    wait_for_completion=True,
    poke_interval=60,
    dag=dag
)

# Monitor Gold completion
monitor_gold_task = PythonOperator(
    task_id='monitor_gold_completion',
    python_callable=lambda **context: monitor_layer_completion('gold', **context),
    dag=dag
)

# Finalize pipeline
finalize_pipeline_task = PythonOperator(
    task_id='finalize_pipeline',
    python_callable=finalize_pipeline_run,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag
)

# Success notification
success_notification_task = DummyOperator(
    task_id='pipeline_success_notification',
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================

initialize_pipeline_task >> trigger_bronze_task >> monitor_bronze_task
monitor_bronze_task >> trigger_silver_task >> monitor_silver_task  
monitor_silver_task >> trigger_gold_task >> monitor_gold_task
monitor_gold_task >> finalize_pipeline_task >> success_notification_task