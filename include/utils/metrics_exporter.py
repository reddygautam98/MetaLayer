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

def start_metrics_collection():
    """Start all metric collection threads"""
    logger.info("Starting MetaLayer metrics collection...")
    
    # Start Prometheus metrics server on port 8000
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")
    
    # Start metric collection threads
    db_thread = threading.Thread(target=simulate_database_metrics, daemon=True)
    quality_thread = threading.Thread(target=simulate_data_quality_metrics, daemon=True)
    processing_thread = threading.Thread(target=simulate_processing_metrics, daemon=True)
    
    db_thread.start()
    quality_thread.start() 
    processing_thread.start()
    
    logger.info("All metric collection threads started")
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(60)
            logger.info("MetaLayer metrics collector running...")
    except KeyboardInterrupt:
        logger.info("Shutting down metrics collector...")

if __name__ == "__main__":
    start_metrics_collection()