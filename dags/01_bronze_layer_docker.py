"""
BRONZE LAYER ETL DAG - DATA INGESTION & RAW DATA PROCESSING
============================================================

This DAG handles the Bronze layer of the medallion architecture:
- Raw data ingestion from multiple source systems
- Data validation and quality checks
- Schema standardization and audit trail creation
- Error handling and data lineage tracking

Features:
- Docker-optimized file paths and configurations
- Comprehensive data quality validation
- Parallel processing for multiple data sources
- Detailed logging and monitoring integration
- Production-ready error handling and recovery
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from utils.connection_pooling import get_db_connection
from utils.data_quality_monitoring import DataQualityValidator
from utils.metrics_exporter import export_pipeline_metrics

# Add include path for utilities
sys.path.append("/opt/airflow/include")

# =====================================================
# CONFIGURATION & CONSTANTS
# =====================================================
DAG_ID = "bronze_layer_etl_pipeline"
DATA_DIR = "/opt/airflow/data/bronze_src"
CHUNK_SIZE = int(Variable.get("bronze_batch_size", default_var=50000))
MAX_ERRORS = int(Variable.get("max_record_errors", default_var=1000))
QUALITY_THRESHOLD = float(Variable.get("data_quality_threshold", default_var=0.95))

# Setup logging
logger = logging.getLogger(__name__)

# =====================================================
# DAG DEFAULT ARGUMENTS
# =====================================================
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email": [Variable.get("alert_email", default_var="data-team@company.com")],
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}


# =====================================================
# CUSTOM EXCEPTIONS
# =====================================================
class DataQualityError(AirflowException):
    """Custom exception for data quality issues"""

    pass


class SourceDataError(AirflowException):
    """Custom exception for source data issues"""

    pass


# =====================================================
# DATA QUALITY & VALIDATION FUNCTIONS
# =====================================================
def validate_source_data(**context) -> Dict[str, Any]:
    """Validate source data files and structure"""
    execution_date = context["execution_date"]

    logger.info(f"🔍 Starting source data validation for {execution_date}")

    validation_results = {
        "crm_data": {"valid": False, "records": 0, "issues": []},
        "erp_data": {"valid": False, "records": 0, "issues": []},
        "file_data": {"valid": False, "records": 0, "issues": []},
    }

    try:
        # Validate CRM data source
        crm_path = Path(DATA_DIR) / "crm"
        if crm_path.exists():
            crm_files = list(crm_path.glob("*.csv"))
            if crm_files:
                for file_path in crm_files:
                    try:
                        df = pd.read_csv(file_path, nrows=1000)  # Sample validation
                        required_cols = ["customer_id", "customer_name", "email"]
                        missing_cols = set(required_cols) - set(df.columns)

                        if not missing_cols:
                            validation_results["crm_data"]["valid"] = True
                            validation_results["crm_data"]["records"] += len(df)
                        else:
                            validation_results["crm_data"]["issues"].append(
                                f"Missing columns in {file_path}: {missing_cols}"
                            )
                    except Exception as e:
                        validation_results["crm_data"]["issues"].append(
                            f"Error reading {file_path}: {str(e)}"
                        )

        # Validate ERP data source
        erp_path = Path(DATA_DIR) / "erp"
        if erp_path.exists():
            erp_files = list(erp_path.glob("*.csv"))
            if erp_files:
                for file_path in erp_files:
                    try:
                        df = pd.read_csv(file_path, nrows=1000)
                        required_cols = [
                            "order_id",
                            "customer_id",
                            "product_id",
                            "order_date",
                        ]
                        missing_cols = set(required_cols) - set(df.columns)

                        if not missing_cols:
                            validation_results["erp_data"]["valid"] = True
                            validation_results["erp_data"]["records"] += len(df)
                        else:
                            validation_results["erp_data"]["issues"].append(
                                f"Missing columns in {file_path}: {missing_cols}"
                            )
                    except Exception as e:
                        validation_results["erp_data"]["issues"].append(
                            f"Error reading {file_path}: {str(e)}"
                        )

        # Check if any valid sources exist
        valid_sources = sum(
            1 for source in validation_results.values() if source["valid"]
        )

        if valid_sources == 0:
            raise SourceDataError("No valid source data found for processing")

        logger.info(f"✅ Validation completed. Valid sources: {valid_sources}")
        logger.info(f"📊 Validation results: {validation_results}")

        # Store validation results for downstream tasks
        context["task_instance"].xcom_push(
            key="validation_results", value=validation_results
        )

        return validation_results

    except Exception as e:
        logger.error(f"❌ Source data validation failed: {str(e)}")
        raise


def determine_processing_branch(**context) -> str:
    """Determine which processing branch to take based on validation results"""

    validation_results = context["task_instance"].xcom_pull(
        task_ids="validate_source_data", key="validation_results"
    )

    if not validation_results:
        logger.error("❌ No validation results found")
        return "skip_processing"

    # Count valid sources
    valid_sources = [
        source for source, data in validation_results.items() if data["valid"]
    ]

    if len(valid_sources) >= 2:
        logger.info(f"✅ Multiple valid sources found: {valid_sources}")
        return "parallel_ingestion"
    elif len(valid_sources) == 1:
        logger.info(f"⚠️ Single valid source found: {valid_sources[0]}")
        return "single_source_ingestion"
    else:
        logger.warning("❌ No valid sources found")
        return "skip_processing"


def ingest_crm_data(**context) -> Dict[str, Any]:
    """Ingest CRM data into Bronze layer"""
    execution_date = context["execution_date"]

    logger.info(f"🏗️ Starting CRM data ingestion for {execution_date}")

    try:
        # Get database connection
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Initialize metrics
        metrics = {
            "records_processed": 0,
            "records_inserted": 0,
            "errors": 0,
            "processing_time": 0,
            "data_quality_score": 0.0,
        }

        start_time = datetime.now()

        # Process CRM files
        crm_path = Path(DATA_DIR) / "crm"
        crm_files = list(crm_path.glob("*.csv"))

        for file_path in crm_files:
            logger.info(f"📄 Processing file: {file_path}")

            # Read CSV in chunks
            chunk_iter = pd.read_csv(file_path, chunksize=CHUNK_SIZE)

            for chunk_idx, chunk in enumerate(chunk_iter):
                try:
                    # Data cleaning and standardization
                    chunk = chunk.dropna(subset=["customer_id", "customer_name"])
                    chunk["customer_id"] = pd.to_numeric(
                        chunk["customer_id"], errors="coerce"
                    )
                    chunk = chunk.dropna(subset=["customer_id"])

                    # Add metadata columns
                    chunk["source_system"] = "CRM"
                    chunk["ingestion_timestamp"] = datetime.now()
                    chunk["file_name"] = file_path.name
                    chunk["chunk_id"] = chunk_idx

                    # Execute batch insert
                    records = chunk.to_dict("records")
                    hook.insert_rows(
                        table="bronze.customers_raw",
                        rows=records,
                        target_fields=[
                            "customer_id",
                            "customer_name",
                            "email",
                            "phone",
                            "address",
                            "created_date",
                            "source_system",
                            "ingestion_timestamp",
                            "file_name",
                            "chunk_id",
                        ],
                        replace=True,
                    )

                    metrics["records_processed"] += len(chunk)
                    metrics["records_inserted"] += len(records)

                    logger.info(
                        f"✅ Processed chunk {chunk_idx}: {len(records)} records"
                    )

                except Exception as e:
                    metrics["errors"] += 1
                    logger.error(f"❌ Error processing chunk {chunk_idx}: {str(e)}")

                    if metrics["errors"] > MAX_ERRORS:
                        raise DataQualityError(
                            f"Too many errors ({metrics['errors']}) exceeded threshold"
                        )

        # Calculate metrics
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        if metrics["records_processed"] > 0:
            metrics["data_quality_score"] = (
                metrics["records_inserted"] / metrics["records_processed"]
            )

        logger.info(f"✅ CRM ingestion completed: {metrics}")

        # Export metrics to monitoring system
        export_pipeline_metrics("bronze_crm_ingestion", metrics, execution_date)

        # Validate quality threshold
        if metrics["data_quality_score"] < QUALITY_THRESHOLD:
            raise DataQualityError(
                f"Data quality score {metrics['data_quality_score']} below threshold {QUALITY_THRESHOLD}"
            )

        return metrics

    except Exception as e:
        logger.error(f"❌ CRM data ingestion failed: {str(e)}")
        raise


def ingest_erp_data(**context) -> Dict[str, Any]:
    """Ingest ERP data into Bronze layer"""
    execution_date = context["execution_date"]

    logger.info(f"🏭 Starting ERP data ingestion for {execution_date}")

    try:
        # Get database connection
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Initialize metrics
        metrics = {
            "records_processed": 0,
            "records_inserted": 0,
            "errors": 0,
            "processing_time": 0,
            "data_quality_score": 0.0,
        }

        start_time = datetime.now()

        # Process ERP files
        erp_path = Path(DATA_DIR) / "erp"
        erp_files = list(erp_path.glob("*.csv"))

        for file_path in erp_files:
            logger.info(f"📄 Processing file: {file_path}")

            # Read CSV in chunks
            chunk_iter = pd.read_csv(file_path, chunksize=CHUNK_SIZE)

            for chunk_idx, chunk in enumerate(chunk_iter):
                try:
                    # Data cleaning and standardization
                    chunk = chunk.dropna(
                        subset=["order_id", "customer_id", "product_id"]
                    )
                    chunk["order_id"] = pd.to_numeric(
                        chunk["order_id"], errors="coerce"
                    )
                    chunk["customer_id"] = pd.to_numeric(
                        chunk["customer_id"], errors="coerce"
                    )
                    chunk = chunk.dropna(subset=["order_id", "customer_id"])

                    # Add metadata columns
                    chunk["source_system"] = "ERP"
                    chunk["ingestion_timestamp"] = datetime.now()
                    chunk["file_name"] = file_path.name
                    chunk["chunk_id"] = chunk_idx

                    # Insert into Bronze layer
                    records = chunk.to_dict("records")
                    hook.insert_rows(
                        table="bronze.orders_raw",
                        rows=records,
                        target_fields=[
                            "order_id",
                            "customer_id",
                            "product_id",
                            "order_date",
                            "quantity",
                            "price",
                            "total_amount",
                            "source_system",
                            "ingestion_timestamp",
                            "file_name",
                            "chunk_id",
                        ],
                        replace=True,
                    )

                    metrics["records_processed"] += len(chunk)
                    metrics["records_inserted"] += len(records)

                    logger.info(
                        f"✅ Processed chunk {chunk_idx}: {len(records)} records"
                    )

                except Exception as e:
                    metrics["errors"] += 1
                    logger.error(f"❌ Error processing chunk {chunk_idx}: {str(e)}")

                    if metrics["errors"] > MAX_ERRORS:
                        raise DataQualityError(
                            f"Too many errors ({metrics['errors']}) exceeded threshold"
                        )

        # Calculate metrics
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        if metrics["records_processed"] > 0:
            metrics["data_quality_score"] = (
                metrics["records_inserted"] / metrics["records_processed"]
            )

        logger.info(f"✅ ERP ingestion completed: {metrics}")

        # Export metrics to monitoring system
        export_pipeline_metrics("bronze_erp_ingestion", metrics, execution_date)

        # Validate quality threshold
        if metrics["data_quality_score"] < QUALITY_THRESHOLD:
            raise DataQualityError(
                f"Data quality score {metrics['data_quality_score']} below threshold {QUALITY_THRESHOLD}"
            )

        return metrics

    except Exception as e:
        logger.error(f"❌ ERP data ingestion failed: {str(e)}")
        raise


def validate_bronze_layer(**context) -> Dict[str, Any]:
    """Validate Bronze layer data quality and completeness"""
    execution_date = context["execution_date"]

    logger.info(f"🔍 Starting Bronze layer validation for {execution_date}")

    try:
        validator = DataQualityValidator("postgres_default")

        # Validate customers table
        customers_validation = validator.validate_data_quality(
            "customers_raw", "bronze"
        )

        # Validate orders table
        orders_validation = validator.validate_data_quality("orders_raw", "bronze")

        # Calculate overall quality score
        customers_score = customers_validation.get("overall_score", 0.0)
        orders_score = orders_validation.get("overall_score", 0.0)
        overall_quality_score = (customers_score + orders_score) / 2

        # Collect issues
        issues_found = []
        if customers_validation.get("overall_status") != "PASSED":
            issues_found.append(
                f"Customers table: {
                    customers_validation.get(
                        'error',
                        'Quality checks failed')}"
            )
        if orders_validation.get("overall_status") != "PASSED":
            issues_found.append(
                f"Orders table: {
                    orders_validation.get(
                        'error',
                        'Quality checks failed')}"
            )

        validation_results = {
            "customers_validation": customers_validation,
            "orders_validation": orders_validation,
            "overall_quality_score": round(overall_quality_score, 3),
            "issues_found": issues_found,
            "validation_time": datetime.now().isoformat(),
        }

        logger.info(
            f"✅ Bronze layer validation completed: Quality Score = {
                overall_quality_score:.2%}"
        )

        if issues_found:
            logger.warning(f"⚠️ Issues found: {issues_found}")
        else:
            logger.info("🎉 All validation checks passed!")

        # Export validation metrics
        export_pipeline_metrics("bronze_validation", validation_results, execution_date)

        return validation_results

    except Exception as e:
        logger.error(f"❌ Bronze layer validation failed: {str(e)}")
        raise


def log_pipeline_completion(**context) -> None:
    """Log pipeline completion with comprehensive metrics"""
    execution_date = context["execution_date"]

    try:
        # Collect metrics from all tasks
        crm_metrics = (
            context["task_instance"].xcom_pull(task_ids="ingest_crm_data") or {}
        )
        erp_metrics = (
            context["task_instance"].xcom_pull(task_ids="ingest_erp_data") or {}
        )
        validation_results = (
            context["task_instance"].xcom_pull(task_ids="validate_bronze_layer") or {}
        )

        # Aggregate metrics
        total_records = crm_metrics.get("records_inserted", 0) + erp_metrics.get(
            "records_inserted", 0
        )
        total_errors = crm_metrics.get("errors", 0) + erp_metrics.get("errors", 0)
        avg_quality_score = (
            crm_metrics.get("data_quality_score", 0)
            + erp_metrics.get("data_quality_score", 0)
            + validation_results.get("overall_quality_score", 0)
        ) / 3

        summary_metrics = {
            "execution_date": execution_date,
            "total_records_processed": total_records,
            "total_errors": total_errors,
            "average_quality_score": avg_quality_score,
            "pipeline_status": "SUCCESS",
        }

        logger.info("🎉 Bronze layer pipeline completed successfully!")
        logger.info(f"📊 Summary metrics: {summary_metrics}")

        # Store summary metrics in database
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.insert_rows(
            table="airflow_meta.etl_run_log",
            rows=[
                (
                    DAG_ID,
                    "bronze_layer_complete",
                    execution_date,
                    datetime.now(),
                    datetime.now(),
                    "SUCCESS",
                    total_records,
                    None,
                )
            ],
            target_fields=[
                "dag_id",
                "task_id",
                "execution_date",
                "start_time",
                "end_time",
                "status",
                "records_processed",
                "error_message",
            ],
        )

        # Export final metrics
        export_pipeline_metrics(
            "bronze_pipeline_summary", summary_metrics, execution_date
        )

    except Exception as e:
        logger.error(f"❌ Error logging pipeline completion: {str(e)}")
        # Don't fail the DAG for logging errors


# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Bronze Layer ETL Pipeline - Raw Data Ingestion with Quality Validation",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    max_active_runs=1,
    concurrency=4,
    catchup=False,
    tags=["etl", "bronze", "ingestion", "data-quality", "production"],
)

# =====================================================
# TASK DEFINITIONS
# =====================================================

# Start task
start_task = DummyOperator(task_id="start_bronze_pipeline", dag=dag)

# Data validation
validate_source_task = PythonOperator(
    task_id="validate_source_data",
    python_callable=validate_source_data,
    dag=dag,
    doc_md="""
    ## Validate Source Data

    Validates the structure and availability of source data files:
    - Checks file existence and readability
    - Validates required columns presence
    - Performs basic data quality checks
    - Determines processing strategy based on available sources
    """,
)

# Branch decision
branch_task = BranchPythonOperator(
    task_id="determine_processing_branch",
    python_callable=determine_processing_branch,
    dag=dag,
)

# Parallel ingestion tasks
ingest_crm_task = PythonOperator(
    task_id="ingest_crm_data",
    python_callable=ingest_crm_data,
    dag=dag,
    doc_md="""
    ## CRM Data Ingestion

    Ingests customer data from CRM systems:
    - Processes CSV files in configurable chunks
    - Applies data cleaning and standardization
    - Inserts into bronze.customers_raw table
    - Tracks data quality metrics and lineage
    """,
)

ingest_erp_task = PythonOperator(
    task_id="ingest_erp_data",
    python_callable=ingest_erp_data,
    dag=dag,
    doc_md="""
    ## ERP Data Ingestion

    Ingests order data from ERP systems:
    - Processes CSV files in configurable chunks
    - Applies data cleaning and validation
    - Inserts into bronze.orders_raw table
    - Tracks processing metrics and data lineage
    """,
)

# Single source ingestion (alternative path)
single_source_task = DummyOperator(task_id="single_source_ingestion", dag=dag)

# Skip processing (no valid sources)
skip_task = DummyOperator(task_id="skip_processing", dag=dag)

# Parallel processing join
parallel_join_task = DummyOperator(
    task_id="parallel_ingestion",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# Data validation
validate_bronze_task = PythonOperator(
    task_id="validate_bronze_layer",
    python_callable=validate_bronze_layer,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
    doc_md="""
    ## Bronze Layer Validation

    Comprehensive validation of ingested data:
    - Record count validation
    - Data quality checks (nulls, duplicates, etc.)
    - Cross-table relationship validation
    - Overall quality score calculation
    """,
)

# Completion logging
completion_task = PythonOperator(
    task_id="log_pipeline_completion",
    python_callable=log_pipeline_completion,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id="end_bronze_pipeline",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================

# Main flow
start_task >> validate_source_task >> branch_task

# Branching paths
branch_task >> [ingest_crm_task, ingest_erp_task] >> parallel_join_task
branch_task >> single_source_task >> parallel_join_task
branch_task >> skip_task >> end_task

# Validation and completion
parallel_join_task >> validate_bronze_task >> completion_task >> end_task

# Alternative flows for single source
single_source_task >> validate_bronze_task
