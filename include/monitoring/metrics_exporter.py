"""
MetaLayer Prometheus Metrics Exporter
This module exports custom metrics for the MetaLayer data pipeline.
"""

import time
import logging
from prometheus_client import start_http_server, Gauge, Counter, Histogram, Info
import threading
import os
from datetime import datetime

# Lazy import for optional dependencies - only imported when needed
_psycopg2 = None
_RealDictCursor = None

def _get_psycopg2_imports():
    """Lazy import psycopg2 modules only when needed."""
    global _psycopg2, _RealDictCursor
    if _psycopg2 is None:
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            _psycopg2 = psycopg2
            _RealDictCursor = RealDictCursor
        except ImportError as e:
            raise RuntimeError(f"psycopg2 is required for database operations: {e}")
    return _psycopg2, _RealDictCursor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
metalayer_db_pool_active_connections = Gauge(
    "metalayer_db_pool_active_connections",
    "Number of active database connections",
    ["pool_name", "database"],
)

metalayer_db_pool_total_connections = Gauge(
    "metalayer_db_pool_total_connections",
    "Total number of connections in the pool",
    ["pool_name", "database"],
)

metalayer_data_quality_score = Gauge(
    "metalayer_data_quality_score",
    "Data quality score for each layer and table",
    ["layer", "table", "check_type"],
)

metalayer_records_processed_total = Counter(
    "metalayer_records_processed_total",
    "Total number of records processed",
    ["layer", "table", "operation"],
)

metalayer_incremental_processing_duration_seconds = Histogram(
    "metalayer_incremental_processing_duration_seconds",
    "Time spent processing incremental data",
    ["layer", "operation", "table"],
    buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

metalayer_pipeline_info = Info(
    "metalayer_pipeline_info", "Information about the MetaLayer pipeline"
)


class MetaLayerMetricsCollector:
    """Collects and exports MetaLayer metrics to Prometheus."""

    def __init__(self, db_config=None):
        self.db_config = db_config or self._get_db_config()
        self.running = False
        self._setup_pipeline_info()

    def _get_db_config(self):
        """Get database configuration from environment variables."""
        return {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "database": os.getenv("POSTGRES_DB", "airflow"),
            "user": os.getenv("POSTGRES_USER", "airflow"),
            "password": os.getenv("POSTGRES_PASSWORD", "airflow_password"),
        }

    def _setup_pipeline_info(self):
        """Set up static pipeline information."""
        metalayer_pipeline_info.info(
            {
                "version": "1.0.0",
                "environment": os.getenv("ENVIRONMENT", "development"),
                "deployment_time": datetime.now().isoformat(),
                "architecture": "medallion",
            }
        )

    def _get_db_connection(self):
        """Get a database connection."""
        try:
            psycopg2, _ = _get_psycopg2_imports()
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return None

    def _collect_connection_pool_metrics(self):
        """Collect database connection pool metrics."""
        try:
            conn = self._get_db_connection()
            if not conn:
                return

            _, real_dict_cursor = _get_psycopg2_imports()
            with conn.cursor(cursor_factory=real_dict_cursor) as cur:
                # Get active connections
                cur.execute(
                    """
                    SELECT 
                        state,
                        COUNT(*) as count
                    FROM pg_stat_activity 
                    WHERE datname = %s
                    GROUP BY state
                """,
                    (self.db_config["database"],),
                )

                active_connections = 0
                total_connections = 0

                for row in cur.fetchall():
                    count = row["count"]
                    if row["state"] == "active":
                        active_connections = count
                    total_connections += count

                # Set metrics
                metalayer_db_pool_active_connections.labels(
                    pool_name="main", database=self.db_config["database"]
                ).set(active_connections)

                metalayer_db_pool_total_connections.labels(
                    pool_name="main", database=self.db_config["database"]
                ).set(total_connections)

            conn.close()

        except Exception as e:
            logger.error(f"Error collecting connection pool metrics: {e}")

    def _collect_data_quality_metrics(self):
        """Collect data quality metrics."""
        try:
            conn = self._get_db_connection()
            if not conn:
                return

            _, real_dict_cursor = _get_psycopg2_imports()
            with conn.cursor(cursor_factory=real_dict_cursor) as cur:
                # Check each layer for basic data quality
                layers = ["bronze", "silver", "gold"]

                for layer in layers:
                    try:
                        # Get table count and record counts
                        cur.execute(
                            f"""
                            SELECT 
                                schemaname,
                                tablename,
                                n_tup_ins + n_tup_upd + n_tup_del as total_activity
                            FROM pg_stat_user_tables 
                            WHERE schemaname = %s
                        """,
                            (layer,),
                        )

                        tables = cur.fetchall()

                        if tables:
                            # Calculate quality score based on activity and completeness
                            for table in tables:
                                table_name = table["tablename"]
                                activity = table["total_activity"] or 0

                                # Simple quality score: 0.9 if has recent activity, 0.7 otherwise
                                quality_score = 0.9 if activity > 0 else 0.7

                                metalayer_data_quality_score.labels(
                                    layer=layer,
                                    table=table_name,
                                    check_type="completeness",
                                ).set(quality_score)
                        else:
                            # No tables in layer
                            metalayer_data_quality_score.labels(
                                layer=layer,
                                table="no_tables",
                                check_type="completeness",
                            ).set(0.0)

                    except Exception as e:
                        logger.warning(f"Could not check layer {layer}: {e}")
                        # Set default quality score for missing layers
                        metalayer_data_quality_score.labels(
                            layer=layer,
                            table="not_available",
                            check_type="completeness",
                        ).set(0.5)

            conn.close()

        except Exception as e:
            logger.error(f"Error collecting data quality metrics: {e}")

    def _collect_processing_metrics(self):
        """Collect processing metrics (simulated for demo)."""
        try:
            layers = ["bronze", "silver", "gold"]
            tables = ["customers", "orders", "products", "transactions"]

            for layer in layers:
                for table in tables:
                    # Simulate some processing activity
                    import random

                    # Simulate records processed (increment counter)
                    records_count = random.randint(10, 1000)
                    metalayer_records_processed_total.labels(
                        layer=layer, table=table, operation="transform"
                    )._value._value += records_count

                    # Simulate processing duration
                    duration = random.uniform(0.5, 30.0)
                    metalayer_incremental_processing_duration_seconds.labels(
                        layer=layer, operation="incremental_load", table=table
                    ).observe(duration)

        except Exception as e:
            logger.error(f"Error collecting processing metrics: {e}")

    def collect_metrics(self):
        """Collect all metrics."""
        logger.info("Collecting MetaLayer metrics...")

        self._collect_connection_pool_metrics()
        self._collect_data_quality_metrics()
        self._collect_processing_metrics()

        logger.info("Metrics collection completed")

    def start_metrics_server(self, port=8000):
        """Start the Prometheus metrics server."""
        try:
            start_http_server(port)
            logger.info(f"Metrics server started on port {port}")

            self.running = True

            # Collect metrics every 30 seconds
            while self.running:
                self.collect_metrics()
                time.sleep(30)

        except Exception as e:
            logger.error(f"Error starting metrics server: {e}")

    def stop(self):
        """Stop the metrics collector."""
        self.running = False
        logger.info("Metrics collector stopped")


def main():
    """Main function to run the metrics exporter."""
    logger.info("Starting MetaLayer Prometheus Metrics Exporter")

    # Create collector
    collector = MetaLayerMetricsCollector()

    try:
        # Start metrics server in a separate thread
        metrics_thread = threading.Thread(
            target=collector.start_metrics_server, args=(8000,)
        )
        metrics_thread.daemon = True
        metrics_thread.start()

        logger.info("MetaLayer metrics exporter is running...")
        logger.info("Metrics available at http://localhost:8000/metrics")

        # Keep the main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down metrics exporter...")
        collector.stop()


if __name__ == "__main__":
    main()
