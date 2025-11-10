"""
Simple metrics exporter stub for DAG imports
"""

import logging

logger = logging.getLogger(__name__)

def export_pipeline_metrics(pipeline_name, metrics, execution_date):
    """Simple metrics logging function"""
    logger.info(f"Pipeline: {pipeline_name}, Metrics: {metrics}, Date: {execution_date}")