"""
MetaLayer Prometheus Metrics Exporter
Collects and exposes custom metrics for Grafana dashboards
"""
import time
import random
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define Prometheus metrics
db_pool_active_connections = Gauge(
    'metalayer_db_pool_active_connections',
    'Number of active database connections',
    ['pool_name']
)

db_pool_total_connections = Gauge(
    'metalayer_db_pool_total_connections', 
    'Total database connection pool size',
    ['pool_name']
)

data_quality_score = Gauge(
    'metalayer_data_quality_score',
    'Data quality score from 0 to 1',
    ['layer', 'table']
)

incremental_processing_duration = Histogram(
    'metalayer_incremental_processing_duration_seconds',
    'Duration of incremental processing operations',
    ['layer', 'table', 'operation'],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0]
)

records_processed_total = Counter(
    'metalayer_records_processed_total',
    'Total number of records processed',
    ['layer', 'table', 'operation']
)

def simulate_database_metrics():
    """Simulate realistic database connection pool metrics"""
    while True:
        try:
            # Simulate PostgreSQL connection pool
            active_connections = random.randint(2, 15)
            total_pool_size = 20
            
            db_pool_active_connections.labels(pool_name='postgres_main').set(active_connections)
            db_pool_total_connections.labels(pool_name='postgres_main').set(total_pool_size)
            
            logger.info(f"Updated DB metrics - Active: {active_connections}/{total_pool_size}")
            time.sleep(10)  # Update every 10 seconds
            
        except Exception as e:
            logger.error(f"Error updating database metrics: {e}")
            time.sleep(30)

def simulate_data_quality_metrics():
    """Simulate data quality scores for different layers"""
    layers = ['bronze', 'silver', 'gold']
    tables = ['customers', 'orders', 'products', 'sales']
    
    while True:
        try:
            for layer in layers:
                for table in tables:
                    # Generate realistic quality scores (85-99%)
                    quality_score = random.uniform(0.85, 0.99)
                    data_quality_score.labels(layer=layer, table=table).set(quality_score)
            
            logger.info("Updated data quality metrics")
            time.sleep(30)  # Update every 30 seconds
            
        except Exception as e:
            logger.error(f"Error updating quality metrics: {e}")
            time.sleep(60)

def simulate_processing_metrics():
    """Simulate processing duration and record counts"""
    layers = ['bronze', 'silver', 'gold']
    tables = ['customers', 'orders', 'products']
    operations = ['load', 'transform', 'validate']
    
    while True:
        try:
            for layer in layers:
                for table in tables:
                    for operation in operations:
                        # Simulate processing duration
                        duration = random.uniform(1.0, 30.0)
                        incremental_processing_duration.labels(
                            layer=layer, 
                            table=table, 
                            operation=operation
                        ).observe(duration)
                        
                        # Simulate records processed
                        records_count = random.randint(100, 5000)
                        records_processed_total.labels(
                            layer=layer,
                            table=table, 
                            operation=operation
                        ).inc(records_count)
            
            logger.info("Updated processing metrics")
            time.sleep(45)  # Update every 45 seconds
            
        except Exception as e:
            logger.error(f"Error updating processing metrics: {e}")
            time.sleep(90)

def collect_real_etl_metrics():
    """Collect real ETL metrics from database and Airflow"""
    logger.info("Collecting real ETL metrics...")
    
    try:
        # Database connection for metrics collection
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'metalayer_etl'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        
        while True:
            try:
                with psycopg2.connect(**conn_params) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        # Get record counts per layer
                        layers_tables = [
                            ('bronze', 'customers_raw'),
                            ('bronze', 'orders_raw'),
                            ('silver', 'customers_cleaned'),
                            ('silver', 'orders_cleaned'),
                            ('gold', 'dim_customer'),
                            ('gold', 'fact_orders'),
                            ('gold', 'agg_daily_sales')
                        ]
                        
                        for layer, table in layers_tables:
                            try:
                                cur.execute(f"SELECT COUNT(*) FROM {layer}.{table}")
                                count = cur.fetchone()[0]
                                
                                # Update records processed metric
                                metalayer_records_processed_total.labels(
                                    layer=layer,
                                    table=table,
                                    operation='processed'
                                )._value._value = count
                                
                                # Simulate data quality scores
                                quality_score = min(1.0, 0.85 + (count % 100) * 0.001)
                                metalayer_data_quality_score.labels(
                                    layer=layer,
                                    table=table,
                                    check_type='completeness'
                                ).set(quality_score)
                                
                            except Exception as e:
                                logger.warning(f"Could not collect metrics for {layer}.{table}: {e}")
                        
                        # Database pool metrics (simulated but realistic)
                        cur.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
                        active_connections = cur.fetchone()[0]
                        
                        metalayer_db_pool_active_connections.labels(
                            pool_name='postgres_main',
                            database='metalayer_etl'
                        ).set(active_connections)
                        
                        metalayer_db_pool_total_connections.labels(
                            pool_name='postgres_main',
                            database='metalayer_etl'
                        ).set(50)  # Max pool size
                        
                        logger.info(f"Updated metrics - Active connections: {active_connections}")
                        
            except Exception as e:
                logger.error(f"Error collecting ETL metrics: {e}")
            
            time.sleep(30)  # Collect every 30 seconds
            
    except Exception as e:
        logger.error(f"Fatal error in metrics collection: {e}")

def simulate_processing_metrics():
    """Simulate processing duration metrics"""
    layers = ['bronze', 'silver', 'gold']
    operations = ['extract', 'transform', 'load', 'validate']
    tables = ['customers', 'orders', 'products']
    
    while True:
        try:
            for layer in layers:
                for operation in operations:
                    for table in tables:
                        # Simulate realistic processing times
                        duration = 0.5 + (hash(f"{layer}{operation}{table}") % 100) * 0.01
                        
                        metalayer_incremental_processing_duration_seconds.labels(
                            layer=layer,
                            operation=operation,
                            table=table
                        ).observe(duration)
            
            logger.info("Updated processing duration metrics")
            time.sleep(45)  # Update every 45 seconds
            
        except Exception as e:
            logger.error(f"Error simulating processing metrics: {e}")
            time.sleep(90)

def start_metrics_collection():
    """Start all metric collection threads"""
    logger.info("Starting MetaLayer ETL metrics collection...")
    
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Start metric collection threads
    real_metrics_thread = threading.Thread(target=collect_real_etl_metrics, daemon=True)
    processing_thread = threading.Thread(target=simulate_processing_metrics, daemon=True)
    
    real_metrics_thread.start()
    processing_thread.start()
    
    logger.info("All metric collection threads started")
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(60)
            logger.info("MetaLayer ETL metrics collector running...")
    except KeyboardInterrupt:
        logger.info("Shutting down metrics collector...")

def export_pipeline_metrics(dag_id, task_id, status, execution_time=None, records_processed=0):
    """
    Export pipeline execution metrics to Prometheus
    
    Args:
        dag_id: Airflow DAG identifier
        task_id: Airflow task identifier  
        status: Task execution status (success, failed, running)
        execution_time: Task execution duration in seconds
        records_processed: Number of records processed by the task
    """
    try:
        logger.info(f"Exporting metrics for {dag_id}.{task_id}: status={status}")
        
        # Update records processed counter
        if records_processed > 0:
            metalayer_records_processed_total.labels(
                layer=dag_id.replace('_layer_etl_pipeline', '').replace('_', ''),
                table=task_id,
                operation=status
            ).inc(records_processed)
        
        # Update execution time histogram
        if execution_time:
            metalayer_incremental_processing_duration_seconds.labels(
                layer=dag_id.replace('_layer_etl_pipeline', '').replace('_', ''),
                operation=status,
                table=task_id
            ).observe(execution_time)
            
        logger.info(f"Successfully exported metrics for {dag_id}.{task_id}")
        
    except Exception as e:
        logger.error(f"Error exporting pipeline metrics: {e}")

if __name__ == "__main__":
    start_metrics_collection()