"""
SILVER LAYER ETL DAG - DATA TRANSFORMATION & BUSINESS LOGIC
============================================================

This DAG handles the Silver layer of the medallion architecture:
- Data transformation and cleaning from Bronze to Silver
- Business rule application and standardization
- Data type conversions and schema enforcement
- Incremental processing and change data capture
- Advanced data quality validation

Features:
- Docker-optimized configurations
- Incremental processing with CDC
- Business rule engine integration
- Comprehensive data profiling and monitoring
- Error handling with data lineage tracking
"""

from utils.metrics_exporter import export_pipeline_metrics
from utils.incremental_processing import IncrementalProcessor
from utils.data_quality_monitoring import DataQualityValidator
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# Add include path for utilities
sys.path.append("/opt/airflow/include")

# =====================================================
# CONFIGURATION & CONSTANTS
# =====================================================
DAG_ID = "silver_layer_etl_pipeline"
BATCH_SIZE = int(Variable.get("silver_batch_size", default_var=25000))
QUALITY_THRESHOLD = float(Variable.get("data_quality_threshold", default_var=0.95))
MAX_PROCESSING_TIME = int(
    Variable.get("silver_max_processing_minutes", default_var=120)
)

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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=MAX_PROCESSING_TIME),
    "email": [Variable.get("alert_email", default_var="data-team@company.com")],
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}


# =====================================================
# CUSTOM EXCEPTIONS
# =====================================================
class DataTransformationError(AirflowException):
    """Custom exception for data transformation issues"""

    pass


class BusinessRuleError(AirflowException):
    """Custom exception for business rule validation failures"""

    pass


# =====================================================
# BUSINESS RULE FUNCTIONS
# =====================================================
def apply_customer_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business rules to customer data"""

    logger.info(f"📋 Applying customer business rules to {len(df)} records")

    # Email standardization
    df["email"] = df["email"].str.lower().str.strip()
    df["email_valid"] = df["email"].str.contains(
        r"^[^@]+@[^@]+\.[^@]+$", regex=True, na=False
    )

    # Phone standardization (remove non-digits, format)
    df["phone_clean"] = df["phone"].astype(str).str.replace(r"[^\d]", "", regex=True)
    df["phone_valid"] = df["phone_clean"].str.len().between(10, 15)

    # Name standardization
    df["customer_name_clean"] = df["customer_name"].str.title().str.strip()
    df["name_valid"] = df["customer_name_clean"].str.len() >= 2

    # Data quality score calculation
    quality_checks = ["email_valid", "phone_valid", "name_valid"]
    df["data_quality_score"] = df[quality_checks].sum(axis=1) / len(quality_checks)

    # Mark active customers (quality score >= 0.5)
    df["is_active"] = df["data_quality_score"] >= 0.5

    logger.info(f"✅ Business rules applied. Active customers: {df['is_active'].sum()}")

    return df


def apply_order_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business rules to order data"""

    logger.info(f"📋 Applying order business rules to {len(df)} records")

    # Date validation and standardization
    df["order_date_clean"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["date_valid"] = df["order_date_clean"].notna()

    # Amount validation
    df["unit_price_clean"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity_clean"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["total_amount_clean"] = df["unit_price_clean"] * df["quantity_clean"]

    # Business validations
    df["price_valid"] = (df["unit_price_clean"] > 0) & (df["unit_price_clean"] < 100000)
    df["quantity_valid"] = (df["quantity_clean"] > 0) & (df["quantity_clean"] < 1000)
    df["amount_valid"] = df["total_amount_clean"] > 0

    # Order category based on amount
    df["order_category"] = pd.cut(
        df["total_amount_clean"],
        bins=[0, 100, 500, 1000, float("inf")],
        labels=["Small", "Medium", "Large", "Enterprise"],
        include_lowest=True,
    )

    # Data quality score
    quality_checks = ["date_valid", "price_valid", "quantity_valid", "amount_valid"]
    df["data_quality_score"] = df[quality_checks].sum(axis=1) / len(quality_checks)

    # Mark valid orders
    df["is_valid"] = df["data_quality_score"] >= 0.75

    logger.info(f"✅ Business rules applied. Valid orders: {df['is_valid'].sum()}")

    return df


# =====================================================
# TRANSFORMATION FUNCTIONS
# =====================================================
def transform_customers_bronze_to_silver(**context) -> Dict[str, Any]:
    """Transform customer data from Bronze to Silver layer"""
    execution_date = context["execution_date"]

    logger.info(f"🔄 Starting customer transformation for {execution_date}")

    try:
        # Get database connection
        hook = PostgresHook(postgres_conn_id="postgres_default")
        processor = IncrementalProcessor(hook)

        # Initialize metrics
        metrics = {
            "records_read": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "data_quality_score": 0.0,
            "processing_time": 0,
            "transformation_errors": 0,
        }

        start_time = datetime.now()

        # Get incremental data from Bronze layer
        incremental_query = """
            SELECT DISTINCT
                customer_id, customer_name, email, phone, address,
                created_date, source_system, ingestion_timestamp
            FROM bronze.customers_raw
            WHERE DATE(ingestion_timestamp) = %s
            AND customer_id IS NOT NULL
            ORDER BY customer_id, ingestion_timestamp DESC
        """

        # Execute query and get data in chunks
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(incremental_query, [execution_date.date()])

            # Process in batches
            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                # Convert to DataFrame
                columns = [
                    "customer_id",
                    "customer_name",
                    "email",
                    "phone",
                    "address",
                    "created_date",
                    "source_system",
                    "ingestion_timestamp",
                ]
                df = pd.DataFrame(rows, columns=columns)

                metrics["records_read"] += len(df)

                try:
                    # Apply business rules and transformations
                    df_transformed = apply_customer_business_rules(df)

                    # Add Silver layer metadata
                    df_transformed["processed_timestamp"] = datetime.now()
                    df_transformed["created_date_clean"] = pd.to_datetime(
                        df_transformed["created_date"], errors="coerce"
                    )

                    # Prepare for Silver layer insert
                    silver_columns = [
                        "customer_id",
                        "customer_name_clean",
                        "email",
                        "phone_clean",
                        "address",
                        "created_date_clean",
                        "is_active",
                        "data_quality_score",
                        "processed_timestamp",
                    ]

                    df_final = df_transformed[
                        df_transformed["data_quality_score"]
                        >= 0.3  # Minimum quality threshold
                    ][silver_columns].copy()

                    # Rename columns for Silver schema
                    df_final.columns = [
                        "customer_id",
                        "customer_name",
                        "email",
                        "phone",
                        "address",
                        "created_date",
                        "is_active",
                        "data_quality_score",
                        "processed_timestamp",
                    ]

                    # Insert into Silver layer with upsert logic
                    if not df_final.empty:
                        upsert_query = """
                            INSERT INTO silver.customers_cleaned (
                                customer_id, customer_name, email, phone, address,
                                created_date, is_active, data_quality_score, processed_timestamp
                            ) VALUES %s
                            ON CONFLICT (customer_id) DO UPDATE SET
                                customer_name = EXCLUDED.customer_name,
                                email = EXCLUDED.email,
                                phone = EXCLUDED.phone,
                                address = EXCLUDED.address,
                                is_active = EXCLUDED.is_active,
                                data_quality_score = EXCLUDED.data_quality_score,
                                processed_timestamp = EXCLUDED.processed_timestamp
                        """

                        records = [
                            tuple(row) for row in df_final.itertuples(index=False)
                        ]
                        cursor.execute(upsert_query, records)
                        conn.commit()

                        metrics["records_loaded"] += len(df_final)

                    metrics["records_transformed"] += len(df_transformed)

                except Exception as e:
                    metrics["transformation_errors"] += 1
                    logger.error(f"❌ Error transforming batch: {str(e)}")
                    conn.rollback()

                    if metrics["transformation_errors"] > 10:  # Max allowed errors
                        raise DataTransformationError(
                            f"Too many transformation errors: {
                                metrics['transformation_errors']}")

        # Calculate final metrics
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        if metrics["records_transformed"] > 0:
            metrics["data_quality_score"] = (
                metrics["records_loaded"] / metrics["records_transformed"]
            )

        logger.info(f"✅ Customer transformation completed: {metrics}")

        # Export metrics
        export_pipeline_metrics(
            "silver_customer_transformation", metrics, execution_date
        )

        # Validate quality threshold
        if metrics["data_quality_score"] < QUALITY_THRESHOLD:
            logger.warning(
                f"⚠️ Quality score {
                    metrics['data_quality_score']:.2%} below threshold {
                    QUALITY_THRESHOLD:.2%}")

        return metrics

    except Exception as e:
        logger.error(f"❌ Customer transformation failed: {str(e)}")
        raise


def transform_orders_bronze_to_silver(**context) -> Dict[str, Any]:
    """Transform order data from Bronze to Silver layer"""
    execution_date = context["execution_date"]

    logger.info(f"🛒 Starting order transformation for {execution_date}")

    try:
        # Get database connection
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Initialize metrics
        metrics = {
            "records_read": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "data_quality_score": 0.0,
            "processing_time": 0,
            "transformation_errors": 0,
        }

        start_time = datetime.now()

        # Get incremental data from Bronze layer
        incremental_query = """
            SELECT DISTINCT
                order_id, customer_id, product_id, order_date,
                quantity, price, total_amount, source_system, ingestion_timestamp
            FROM bronze.orders_raw
            WHERE DATE(ingestion_timestamp) = %s
            AND order_id IS NOT NULL
            ORDER BY order_id, ingestion_timestamp DESC
        """

        # Execute query and process in chunks
        with hook.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(incremental_query, [execution_date.date()])

            while True:
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                # Convert to DataFrame
                columns = [
                    "order_id",
                    "customer_id",
                    "product_id",
                    "order_date",
                    "quantity",
                    "price",
                    "total_amount",
                    "source_system",
                    "ingestion_timestamp",
                ]
                df = pd.DataFrame(rows, columns=columns)

                metrics["records_read"] += len(df)

                try:
                    # Apply business rules and transformations
                    df_transformed = apply_order_business_rules(df)

                    # Add Silver layer metadata
                    df_transformed["processed_timestamp"] = datetime.now()

                    # Prepare for Silver layer insert
                    silver_columns = [
                        "order_id",
                        "customer_id",
                        "product_id",
                        "order_date_clean",
                        "quantity_clean",
                        "unit_price_clean",
                        "total_amount_clean",
                        "is_valid",
                        "processed_timestamp",
                    ]

                    df_final = df_transformed[
                        df_transformed["data_quality_score"]
                        >= 0.5  # Quality threshold for orders
                    ][silver_columns].copy()

                    # Rename columns for Silver schema
                    df_final.columns = [
                        "order_id",
                        "customer_id",
                        "product_id",
                        "order_date",
                        "quantity",
                        "unit_price",
                        "total_amount",
                        "is_valid",
                        "processed_timestamp",
                    ]

                    # Insert into Silver layer
                    if not df_final.empty:
                        upsert_query = """
                            INSERT INTO silver.orders_cleaned (
                                order_id, customer_id, product_id, order_date,
                                quantity, unit_price, total_amount, is_valid, processed_timestamp
                            ) VALUES %s
                            ON CONFLICT (order_id) DO UPDATE SET
                                customer_id = EXCLUDED.customer_id,
                                product_id = EXCLUDED.product_id,
                                order_date = EXCLUDED.order_date,
                                quantity = EXCLUDED.quantity,
                                unit_price = EXCLUDED.unit_price,
                                total_amount = EXCLUDED.total_amount,
                                is_valid = EXCLUDED.is_valid,
                                processed_timestamp = EXCLUDED.processed_timestamp
                        """

                        records = [
                            tuple(row) for row in df_final.itertuples(index=False)
                        ]
                        cursor.execute(upsert_query, records)
                        conn.commit()

                        metrics["records_loaded"] += len(df_final)

                    metrics["records_transformed"] += len(df_transformed)

                except Exception as e:
                    metrics["transformation_errors"] += 1
                    logger.error(f"❌ Error transforming batch: {str(e)}")
                    conn.rollback()

                    if metrics["transformation_errors"] > 10:
                        raise DataTransformationError(
                            f"Too many transformation errors: {
                                metrics['transformation_errors']}")

        # Calculate final metrics
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        if metrics["records_transformed"] > 0:
            metrics["data_quality_score"] = (
                metrics["records_loaded"] / metrics["records_transformed"]
            )

        logger.info(f"✅ Order transformation completed: {metrics}")

        # Export metrics
        export_pipeline_metrics("silver_order_transformation", metrics, execution_date)

        return metrics

    except Exception as e:
        logger.error(f"❌ Order transformation failed: {str(e)}")
        raise


def validate_silver_layer_quality(**context) -> Dict[str, Any]:
    """Comprehensive Silver layer data quality validation"""
    execution_date = context["execution_date"]

    logger.info(f"🔍 Starting Silver layer validation for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        validator = DataQualityValidator(hook)

        validation_results = {
            "customer_quality_checks": {},
            "order_quality_checks": {},
            "cross_table_checks": {},
            "overall_quality_score": 0.0,
            "critical_issues": [],
        }

        # Customer quality checks
        customer_checks = [{"name": "customer_record_count",
                            "query": "SELECT COUNT(*) FROM silver.customers_cleaned WHERE DATE(processed_timestamp) = %s",
                            "params": [execution_date.date()],
                            "expected_min": 1,
                            },
                           {"name": "customer_quality_score_avg",
                            "query": "SELECT AVG(data_quality_score) FROM silver.customers_cleaned WHERE DATE(processed_timestamp) = %s",
                            "params": [execution_date.date()],
                            "expected_min": 0.7,
                            },
                           {"name": "active_customer_percentage",
                            "query": """
                    SELECT (COUNT(*) FILTER (WHERE is_active = true)::float / COUNT(*)) * 100
                    FROM silver.customers_cleaned
                    WHERE DATE(processed_timestamp) = %s
                """,
                            "params": [execution_date.date()],
                            "expected_min": 60.0,
                            },
                           ]

        validation_results["customer_quality_checks"] = validator.run_validation_checks(
            customer_checks
        )

        # Order quality checks
        order_checks = [{"name": "order_record_count",
                         "query": "SELECT COUNT(*) FROM silver.orders_cleaned WHERE DATE(processed_timestamp) = %s",
                         "params": [execution_date.date()],
                         "expected_min": 1,
                         },
                        {"name": "valid_order_percentage",
                         "query": """
                    SELECT (COUNT(*) FILTER (WHERE is_valid = true)::float / COUNT(*)) * 100
                    FROM silver.orders_cleaned
                    WHERE DATE(processed_timestamp) = %s
                """,
                         "params": [execution_date.date()],
                         "expected_min": 75.0,
                         },
                        {"name": "average_order_value",
                         "query": "SELECT AVG(total_amount) FROM silver.orders_cleaned WHERE is_valid = true AND DATE(processed_timestamp) = %s",
                         "params": [execution_date.date()],
                         "expected_min": 10.0,
                         },
                        ]

        validation_results["order_quality_checks"] = validator.run_validation_checks(
            order_checks
        )

        # Cross-table relationship checks
        cross_checks = [
            {
                "name": "customer_order_referential_integrity",
                "query": """
                    SELECT COUNT(*) FROM silver.orders_cleaned o
                    LEFT JOIN silver.customers_cleaned c ON o.customer_id = c.customer_id
                    WHERE c.customer_id IS NULL
                    AND DATE(o.processed_timestamp) = %s
                """,
                "params": [execution_date.date()],
                "expected_max": 100,  # Allow some orphaned orders
            }
        ]

        validation_results["cross_table_checks"] = validator.run_validation_checks(
            cross_checks
        )

        # Calculate overall quality score
        all_checks = (
            list(validation_results["customer_quality_checks"].values())
            + list(validation_results["order_quality_checks"].values())
            + list(validation_results["cross_table_checks"].values())
        )

        passed_checks = sum(1 for check in all_checks if check.get("passed", False))
        total_checks = len(all_checks)

        validation_results["overall_quality_score"] = (
            passed_checks / total_checks if total_checks > 0 else 0
        )

        # Collect critical issues
        for check_result in all_checks:
            if not check_result.get("passed", False):
                validation_results["critical_issues"].append(
                    check_result.get("message", "Unknown issue")
                )

        logger.info(
            f"✅ Silver layer validation completed: Quality Score = {
                validation_results['overall_quality_score']:.2%}")

        if validation_results["critical_issues"]:
            logger.warning(
                f"⚠️ Critical issues found: {validation_results['critical_issues']}"
            )

        # Export validation metrics
        export_pipeline_metrics("silver_validation", validation_results, execution_date)

        return validation_results

    except Exception as e:
        logger.error(f"❌ Silver layer validation failed: {str(e)}")
        raise


def create_silver_summary_stats(**context) -> Dict[str, Any]:
    """Create summary statistics for Silver layer"""
    execution_date = context["execution_date"]

    logger.info(f"📊 Creating Silver layer summary stats for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        summary_stats = {}

        # Customer summary stats
        customer_stats_query = """
            SELECT
                COUNT(*) as total_customers,
                COUNT(*) FILTER (WHERE is_active = true) as active_customers,
                AVG(data_quality_score) as avg_quality_score,
                MIN(processed_timestamp) as first_processed,
                MAX(processed_timestamp) as last_processed
            FROM silver.customers_cleaned
            WHERE DATE(processed_timestamp) = %s
        """

        customer_stats = hook.get_first(
            customer_stats_query, parameters=[execution_date.date()]
        )
        if customer_stats:
            summary_stats["customers"] = dict(
                zip(
                    [
                        "total_customers",
                        "active_customers",
                        "avg_quality_score",
                        "first_processed",
                        "last_processed",
                    ],
                    customer_stats,
                )
            )

        # Order summary stats
        order_stats_query = """
            SELECT
                COUNT(*) as total_orders,
                COUNT(*) FILTER (WHERE is_valid = true) as valid_orders,
                SUM(total_amount) FILTER (WHERE is_valid = true) as total_revenue,
                AVG(total_amount) FILTER (WHERE is_valid = true) as avg_order_value,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM silver.orders_cleaned
            WHERE DATE(processed_timestamp) = %s
        """

        order_stats = hook.get_first(
            order_stats_query, parameters=[execution_date.date()]
        )
        if order_stats:
            summary_stats["orders"] = dict(
                zip(
                    [
                        "total_orders",
                        "valid_orders",
                        "total_revenue",
                        "avg_order_value",
                        "unique_customers",
                    ],
                    order_stats,
                )
            )

        logger.info(f"📈 Summary stats created: {summary_stats}")

        # Store summary stats in metadata table
        hook.insert_rows(
            table="airflow_meta.pipeline_performance",
            rows=[
                (
                    DAG_ID,
                    execution_date,
                    summary_stats.get("customers", {}).get("total_customers", 0)
                    + summary_stats.get("orders", {}).get(
                        "total_orders", 0
                    ),  # total_duration_seconds (placeholder)
                    summary_stats.get("orders", {}).get(
                        "avg_order_value", 0
                    ),  # records_per_second (placeholder)
                    0,  # memory_usage_mb (placeholder)
                    0,  # cpu_usage_percent (placeholder)
                )
            ],
            target_fields=[
                "dag_id",
                "execution_date",
                "total_duration_seconds",
                "records_per_second",
                "memory_usage_mb",
                "cpu_usage_percent",
            ],
        )

        return summary_stats

    except Exception as e:
        logger.error(f"❌ Error creating summary stats: {str(e)}")
        raise


# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Silver Layer ETL Pipeline - Data Transformation and Business Logic",
    schedule_interval="0 4 * * *",  # Daily at 4 AM (after Bronze layer)
    max_active_runs=1,
    concurrency=2,
    catchup=False,
    tags=["etl", "silver", "transformation", "business-rules", "production"],
)

# =====================================================
# TASK DEFINITIONS
# =====================================================

# Start task
start_task = DummyOperator(task_id="start_silver_pipeline", dag=dag)


# Check Bronze layer data availability
def check_bronze_data_available(**context):
    """
    Check if there is recent data in Bronze layer tables.
    This is more flexible than ExternalTaskSensor for manual triggers.
    """
    execution_date = context["execution_date"]
    logger.info(f"🔍 Checking Bronze layer data availability for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Check for recent data in bronze tables (within last 24 hours)
        check_date = execution_date.date()

        customers_query = """
            SELECT COUNT(*)
            FROM bronze.customers_raw
            WHERE DATE(ingestion_timestamp) >= %s
        """

        orders_query = """
            SELECT COUNT(*)
            FROM bronze.orders_raw
            WHERE DATE(ingestion_timestamp) >= %s
        """

        customers_count = hook.get_first(customers_query, parameters=[check_date])[0]
        orders_count = hook.get_first(orders_query, parameters=[check_date])[0]

        logger.info(
            f"📊 Found {customers_count} customers and {orders_count} orders for date {check_date}")

        if customers_count > 0 or orders_count > 0:
            logger.info("✅ Bronze data is available for processing")
            return True
        else:
            raise AirflowException(
                f"❌ No Bronze data found for {check_date}. Cannot proceed with Silver layer processing.")

    except Exception as e:
        logger.error(f"❌ Error checking Bronze data availability: {str(e)}")
        raise


wait_for_bronze = PythonOperator(
    task_id="wait_for_bronze_completion",
    python_callable=check_bronze_data_available,
    dag=dag,
    doc_md="""
    ## Check Bronze Data Availability

    Verifies that Bronze layer data is available for processing.
    This approach is more flexible than ExternalTaskSensor for manual triggers
    and mixed scheduling scenarios.
    """,
)

# Customer transformation
transform_customers_task = PythonOperator(
    task_id="transform_customers",
    python_callable=transform_customers_bronze_to_silver,
    dag=dag,
    doc_md="""
    ## Transform Customers

    Transforms customer data from Bronze to Silver layer:
    - Applies business rules and data standardization
    - Calculates data quality scores
    - Performs email and phone validation
    - Creates clean, analytics-ready customer records
    """,
)

# Order transformation
transform_orders_task = PythonOperator(
    task_id="transform_orders",
    python_callable=transform_orders_bronze_to_silver,
    dag=dag,
    doc_md="""
    ## Transform Orders

    Transforms order data from Bronze to Silver layer:
    - Validates order amounts and dates
    - Applies business rules for order categorization
    - Performs data type conversions
    - Creates validated order records
    """,
)

# Silver layer validation
validate_silver_task = PythonOperator(
    task_id="validate_silver_quality",
    python_callable=validate_silver_layer_quality,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
    doc_md="""
    ## Validate Silver Layer Quality

    Comprehensive validation of Silver layer data:
    - Data quality score validation
    - Cross-table referential integrity checks
    - Business rule compliance validation
    - Performance and completeness metrics
    """,
)

# Summary statistics
summary_stats_task = PythonOperator(
    task_id="create_summary_statistics",
    python_callable=create_silver_summary_stats,
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id="end_silver_pipeline", trigger_rule=TriggerRule.NONE_FAILED, dag=dag
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================

start_task >> wait_for_bronze >> [transform_customers_task, transform_orders_task]
[transform_customers_task, transform_orders_task] >> validate_silver_task
validate_silver_task >> summary_stats_task >> end_task
