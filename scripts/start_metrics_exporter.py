#!/usr/bin/env python3
"""
MetaLayer Metrics Exporter Service
Runs the Prometheus metrics exporter as a standalone service.
"""

import sys
import os
import signal
import logging

# Add the include directory to Python path
sys.path.insert(0, '/opt/airflow/include')

from monitoring.metrics_exporter import MetaLayerMetricsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

def main():
    """Main function to start the metrics exporter service."""
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    logger.info("Starting MetaLayer Metrics Exporter Service")
    
    try:
        # Create and start the metrics collector
        collector = MetaLayerMetricsCollector()
        collector.start_metrics_server(port=8000)
        
    except Exception as e:
        logger.error(f"Failed to start metrics exporter: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()