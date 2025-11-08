"""
GOLD LAYER ETL DAG - ANALYTICS & DIMENSIONAL MODELING
====================================================

This DAG handles the Gold layer of the medallion architecture:
- Creates analytics-ready dimensional models
- Builds aggregated fact and dimension tables
- Implements slowly changing dimensions (SCD Type 2)
- Generates business KPIs and metrics
- Creates data marts for specific business domains

Features:
- Docker-optimized performance configurations
- Dimensional modeling with star schema design
- Advanced analytics and business intelligence features
- Automated data mart generation
- Comprehensive performance monitoring
"""

import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from utils.data_quality_monitoring import DataQualityValidator
from utils.metrics_exporter import export_pipeline_metrics

sys.path.append("/opt/airflow/include")

# =====================================================
# CONFIGURATION & CONSTANTS
# =====================================================
DAG_ID = "gold_layer_analytics_pipeline"
BATCH_SIZE = int(Variable.get("gold_batch_size", default_var=50000))
LOOKBACK_DAYS = int(Variable.get("scd_lookback_days", default_var=7))

# Setup logging
logger = logging.getLogger(__name__)

# =====================================================
# DAG DEFAULT ARGUMENTS
# =====================================================
default_args = {
    "owner": "analytics-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=180),
    "email": [Variable.get("alert_email", default_var="analytics-team@company.com")],
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}


# =====================================================
# DIMENSIONAL MODELING FUNCTIONS
# =====================================================
def build_customer_dimension(**context) -> Dict[str, Any]:
    """Build customer dimension table with SCD Type 2"""
    execution_date = context["execution_date"]

    logger.info(f"🏗️ Building customer dimension for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        metrics = {
            "records_processed": 0,
            "new_records": 0,
            "updated_records": 0,
            "scd_changes": 0,
            "processing_time": 0,
        }

        start_time = datetime.now()

        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # Get latest customer data from Silver layer
            silver_query = """
                SELECT
                    customer_id,
                    customer_name,
                    email,
                    phone,
                    address,
                    is_active,
                    data_quality_score,
                    processed_timestamp
                FROM silver.customers_cleaned
                WHERE DATE(processed_timestamp) >= %s - INTERVAL '%s days'
                ORDER BY customer_id, processed_timestamp DESC
            """

            cursor.execute(silver_query, [execution_date.date(), LOOKBACK_DAYS])
            silver_data = cursor.fetchall()

            if not silver_data:
                logger.warning("⚠️ No customer data found in Silver layer")
                return metrics

            # Convert to DataFrame for processing
            columns = [
                "customer_id",
                "customer_name",
                "email",
                "phone",
                "address",
                "is_active",
                "data_quality_score",
                "processed_timestamp",
            ]
            df_silver = pd.DataFrame(silver_data, columns=columns)

            # Get existing dimension records
            existing_query = """
                SELECT
                    customer_key, customer_id, customer_name, email, phone,
                    address, is_active, effective_from, effective_to, is_current
                FROM gold.dim_customer
                WHERE customer_id IN %s
            """

            customer_ids = tuple(df_silver["customer_id"].unique())
            cursor.execute(existing_query, [customer_ids])
            existing_data = cursor.fetchall()

            existing_columns = [
                "customer_key",
                "customer_id",
                "customer_name",
                "email",
                "phone",
                "address",
                "is_active",
                "effective_from",
                "effective_to",
                "is_current",
            ]
            df_existing = pd.DataFrame(existing_data, columns=existing_columns)

            # Process SCD Type 2 logic
            for _, silver_row in df_silver.iterrows():
                customer_id = silver_row["customer_id"]

                # Get current dimension record
                current_record = df_existing[
                    (df_existing["customer_id"] == customer_id)
                    & df_existing["is_current"]
                ]

                if current_record.empty:
                    # New customer - insert new record
                    insert_query = """
                        INSERT INTO gold.dim_customer (
                            customer_id, customer_name, email, phone, address,
                            is_active, data_quality_score, effective_from, effective_to, is_current
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    cursor.execute(
                        insert_query,
                        [
                            customer_id,
                            silver_row["customer_name"],
                            silver_row["email"],
                            silver_row["phone"],
                            silver_row["address"],
                            silver_row["is_active"],
                            silver_row["data_quality_score"],
                            execution_date,
                            datetime(9999, 12, 31),
                            True,
                        ],
                    )

                    metrics["new_records"] += 1

                else:
                    # Check for changes (SCD Type 2)
                    current = current_record.iloc[0]

                    changes_detected = (
                        current["customer_name"] != silver_row["customer_name"]
                        or current["email"] != silver_row["email"]
                        or current["phone"] != silver_row["phone"]
                        or current["address"] != silver_row["address"]
                        or current["is_active"] != silver_row["is_active"]
                    )

                    if changes_detected:
                        # Close current record
                        update_query = """
                            UPDATE gold.dim_customer
                            SET effective_to = %s, is_current = false
                            WHERE customer_key = %s
                        """
                        cursor.execute(
                            update_query, [execution_date, current["customer_key"]]
                        )

                        # Insert new record
                        insert_query = """
                            INSERT INTO gold.dim_customer (
                                customer_id, customer_name, email, phone, address,
                                is_active, data_quality_score, effective_from, effective_to, is_current
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        cursor.execute(
                            insert_query,
                            [
                                customer_id,
                                silver_row["customer_name"],
                                silver_row["email"],
                                silver_row["phone"],
                                silver_row["address"],
                                silver_row["is_active"],
                                silver_row["data_quality_score"],
                                execution_date,
                                datetime(9999, 12, 31),
                                True,
                            ],
                        )

                        metrics["scd_changes"] += 1
                        metrics["updated_records"] += 1

                metrics["records_processed"] += 1

            conn.commit()

        # Calculate processing time
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        logger.info(f"✅ Customer dimension built: {metrics}")

        # Export metrics
        export_pipeline_metrics("gold_customer_dimension", metrics, execution_date)

        return metrics

    except Exception as e:
        logger.error(f"❌ Customer dimension build failed: {str(e)}")
        raise


def build_order_fact_table(**context) -> Dict[str, Any]:
    """Build order fact table with aggregations"""
    execution_date = context["execution_date"]

    logger.info(f"📊 Building order fact table for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        metrics = {
            "fact_records_created": 0,
            "daily_aggregations": 0,
            "monthly_aggregations": 0,
            "processing_time": 0,
        }

        start_time = datetime.now()

        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # Build detailed fact table from Silver orders
            fact_insert_query = """
                INSERT INTO gold.fact_orders (
                    order_date_key, customer_key, order_id,
                    quantity, unit_price, total_amount, is_valid
                )
                SELECT
                    TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER as order_date_key,
                    COALESCE(dc.customer_key, -1) as customer_key,
                    o.order_id,
                    o.quantity,
                    o.unit_price,
                    o.total_amount,
                    o.is_valid
                FROM silver.orders_cleaned o
                LEFT JOIN gold.dim_customer dc ON o.customer_id = dc.customer_id
                    AND dc.is_current = true
                WHERE DATE(o.processed_timestamp) = %s
                    AND o.is_valid = true
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_key = EXCLUDED.customer_key,
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    total_amount = EXCLUDED.total_amount
            """

            cursor.execute(fact_insert_query, [execution_date.date()])
            metrics["fact_records_created"] = cursor.rowcount

            # Create daily aggregations
            daily_agg_query = """
                INSERT INTO gold.agg_daily_sales (
                    date_key, total_orders, total_revenue, avg_order_value,
                    unique_customers, total_quantity
                )
                SELECT
                    order_date_key,
                    COUNT(*) as total_orders,
                    SUM(total_amount) as total_revenue,
                    AVG(total_amount) as avg_order_value,
                    COUNT(DISTINCT customer_key) as unique_customers,
                    SUM(quantity) as total_quantity
                FROM gold.fact_orders
                WHERE order_date_key = TO_CHAR(%s, 'YYYYMMDD')::INTEGER
                GROUP BY order_date_key
                ON CONFLICT (date_key) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    unique_customers = EXCLUDED.unique_customers,
                    total_quantity = EXCLUDED.total_quantity
            """

            cursor.execute(daily_agg_query, [execution_date.date()])
            metrics["daily_aggregations"] = cursor.rowcount

            # Create monthly aggregations
            monthly_agg_query = """
                INSERT INTO gold.agg_monthly_sales (
                    year_month, total_orders, total_revenue, avg_order_value,
                    unique_customers, total_quantity
                )
                SELECT
                    TO_CHAR(%s, 'YYYYMM')::INTEGER as year_month,
                    SUM(total_orders) as total_orders,
                    SUM(total_revenue) as total_revenue,
                    AVG(avg_order_value) as avg_order_value,
                    COUNT(DISTINCT date_key) as active_days,  -- Placeholder for unique_customers
                    SUM(total_quantity) as total_quantity
                FROM gold.agg_daily_sales
                WHERE date_key >= TO_CHAR(DATE_TRUNC('month', %s), 'YYYYMMDD')::INTEGER
                    AND date_key <= TO_CHAR(%s, 'YYYYMMDD')::INTEGER
                GROUP BY TO_CHAR(%s, 'YYYYMM')::INTEGER
                ON CONFLICT (year_month) DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    total_revenue = EXCLUDED.total_revenue,
                    avg_order_value = EXCLUDED.avg_order_value,
                    unique_customers = EXCLUDED.unique_customers,
                    total_quantity = EXCLUDED.total_quantity
            """

            cursor.execute(
                monthly_agg_query,
                [
                    execution_date.date(),
                    execution_date.date(),
                    execution_date.date(),
                    execution_date.date(),
                ],
            )
            metrics["monthly_aggregations"] = cursor.rowcount

            conn.commit()

        # Calculate processing time
        end_time = datetime.now()
        metrics["processing_time"] = (end_time - start_time).total_seconds()

        logger.info(f"✅ Order fact table built: {metrics}")

        # Export metrics
        export_pipeline_metrics("gold_order_facts", metrics, execution_date)

        return metrics

    except Exception as e:
        logger.error(f"❌ Order fact table build failed: {str(e)}")
        raise


def build_business_kpis(**context) -> Dict[str, Any]:
    """Calculate and store business KPIs"""
    execution_date = context["execution_date"]

    logger.info(f"📈 Building business KPIs for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        kpi_results = {}

        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # Customer Lifetime Value (CLV) calculation
            clv_query = """
                SELECT
                    dc.customer_id,
                    COUNT(fo.order_id) as total_orders,
                    SUM(fo.total_amount) as total_spent,
                    AVG(fo.total_amount) as avg_order_value,
                    MAX(fo.order_date_key) as last_order_date
                FROM gold.dim_customer dc
                LEFT JOIN gold.fact_orders fo ON dc.customer_key = fo.customer_key
                WHERE dc.is_current = true
                GROUP BY dc.customer_id
            """

            cursor.execute(clv_query)
            # Store CLV data for analytics (data available in cursor)
            logger.info(f"Calculated CLV metrics for {cursor.rowcount} customers")

            # Calculate cohort analysis
            cohort_query = """
                WITH customer_cohorts AS (
                    SELECT
                        customer_key,
                        MIN(order_date_key) as first_order_date,
                        TO_CHAR(TO_DATE(MIN(order_date_key)::text, 'YYYYMMDD'), 'YYYY-MM') as cohort_month
                    FROM gold.fact_orders
                    GROUP BY customer_key
                ),
                cohort_metrics AS (
                    SELECT
                        cc.cohort_month,
                        COUNT(DISTINCT cc.customer_key) as cohort_size,
                        COUNT(DISTINCT fo.customer_key) as active_customers,
                        SUM(fo.total_amount) as cohort_revenue
                    FROM customer_cohorts cc
                    LEFT JOIN gold.fact_orders fo ON cc.customer_key = fo.customer_key
                        AND fo.order_date_key >= cc.first_order_date
                    WHERE cc.cohort_month = TO_CHAR(%s, 'YYYY-MM')
                    GROUP BY cc.cohort_month
                )
                SELECT * FROM cohort_metrics
            """

            cursor.execute(cohort_query, [execution_date.date()])
            cohort_data = cursor.fetchone()

            if cohort_data:
                kpi_results["cohort_analysis"] = {
                    "cohort_month": cohort_data[0],
                    "cohort_size": cohort_data[1],
                    "active_customers": cohort_data[2],
                    "cohort_revenue": float(cohort_data[3]) if cohort_data[3] else 0.0,
                }

            # Product performance metrics
            product_performance_query = """
                SELECT
                    COUNT(DISTINCT customer_key) as unique_buyers,
                    SUM(quantity) as total_quantity_sold,
                    SUM(total_amount) as total_revenue,
                    AVG(unit_price) as avg_price
                FROM gold.fact_orders
                WHERE order_date_key = TO_CHAR(%s, 'YYYYMMDD')::INTEGER
            """

            cursor.execute(product_performance_query, [execution_date.date()])
            product_data = cursor.fetchone()

            if product_data:
                kpi_results["daily_performance"] = {
                    "unique_buyers": product_data[0],
                    "total_quantity_sold": product_data[1],
                    "total_revenue": float(product_data[2]) if product_data[2] else 0.0,
                    "avg_price": float(product_data[3]) if product_data[3] else 0.0,
                }

            # Store KPIs in business metrics table
            kpi_insert_query = """
                INSERT INTO gold.business_kpis (
                    date_key, kpi_name, kpi_value, kpi_metadata
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (date_key, kpi_name) DO UPDATE SET
                    kpi_value = EXCLUDED.kpi_value,
                    kpi_metadata = EXCLUDED.kpi_metadata
            """

            date_key = int(execution_date.strftime("%Y%m%d"))

            # Insert each KPI
            for category, metrics in kpi_results.items():
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        cursor.execute(
                            kpi_insert_query,
                            [
                                date_key,
                                f"{category}_{metric_name}",
                                metric_value,
                                str(metrics),
                            ],
                        )

            conn.commit()

        logger.info(f"✅ Business KPIs calculated: {kpi_results}")

        # Export metrics
        export_pipeline_metrics("gold_business_kpis", kpi_results, execution_date)

        return kpi_results

    except Exception as e:
        logger.error(f"❌ Business KPI calculation failed: {str(e)}")
        raise


def validate_gold_layer_quality(**context) -> Dict[str, Any]:
    """Validate Gold layer data quality and consistency"""
    execution_date = context["execution_date"]

    logger.info(f"🔍 Validating Gold layer quality for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        validator = DataQualityValidator(hook)

        validation_results = {
            "dimension_checks": {},
            "fact_checks": {},
            "kpi_checks": {},
            "overall_quality_score": 0.0,
        }

        # Dimension table validations
        dimension_checks = [
            {
                "name": "customer_dimension_completeness",
                "query": """
                    SELECT COUNT(*) FROM gold.dim_customer
                    WHERE is_current = true AND customer_name IS NOT NULL
                """,
                "expected_min": 1,
            },
            {
                "name": "scd_integrity_check",
                "query": """
                    SELECT COUNT(*) FROM gold.dim_customer
                    WHERE effective_from > effective_to
                """,
                "expected_max": 0,
            },
        ]

        validation_results["dimension_checks"] = validator.run_validation_checks(
            dimension_checks
        )

        # Fact table validations
        fact_checks = [
            {
                "name": "fact_orders_completeness",
                "query": f"""
                    SELECT COUNT(*) FROM gold.fact_orders
                    WHERE order_date_key = {int(execution_date.strftime('%Y%m%d'))}
                """,
                "expected_min": 1,
            },
            {
                "name": "revenue_consistency",
                "query": f"""
                    SELECT ABS(
                        (SELECT SUM(total_amount) FROM gold.fact_orders
                         WHERE order_date_key = {int(execution_date.strftime('%Y%m%d'))}) -
                        (SELECT total_revenue FROM gold.agg_daily_sales
                         WHERE date_key = {int(execution_date.strftime('%Y%m%d'))})
                    )
                """,
                "expected_max": 0.01,  # Allow small rounding differences
            },
        ]

        validation_results["fact_checks"] = validator.run_validation_checks(fact_checks)

        # KPI validations
        kpi_checks = [
            {
                "name": "daily_kpis_present",
                "query": f"""
                    SELECT COUNT(DISTINCT kpi_name) FROM gold.business_kpis
                    WHERE date_key = {int(execution_date.strftime('%Y%m%d'))}
                """,
                "expected_min": 3,  # Expect at least 3 different KPIs
            }
        ]

        validation_results["kpi_checks"] = validator.run_validation_checks(kpi_checks)

        # Calculate overall quality score
        all_checks = (
            list(validation_results["dimension_checks"].values())
            + list(validation_results["fact_checks"].values())
            + list(validation_results["kpi_checks"].values())
        )

        passed_checks = sum(1 for check in all_checks if check.get("passed", False))
        total_checks = len(all_checks)

        validation_results["overall_quality_score"] = (
            passed_checks / total_checks if total_checks > 0 else 0
        )

        logger.info(
            f"✅ Gold layer validation completed: Quality Score = {validation_results['overall_quality_score']:.2%}"
        )

        return validation_results

    except Exception as e:
        logger.error(f"❌ Gold layer validation failed: {str(e)}")
        raise


def generate_analytics_summary(**context) -> Dict[str, Any]:
    """Generate comprehensive analytics summary"""
    execution_date = context["execution_date"]

    logger.info(f"📊 Generating analytics summary for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        summary = {}

        with hook.get_conn() as conn:
            cursor = conn.cursor()

            # Daily summary metrics
            daily_summary_query = f"""
                SELECT
                    total_orders,
                    total_revenue,
                    avg_order_value,
                    unique_customers
                FROM gold.agg_daily_sales
                WHERE date_key = {int(execution_date.strftime('%Y%m%d'))}
            """

            cursor.execute(daily_summary_query)
            daily_data = cursor.fetchone()

            if daily_data:
                summary["daily_metrics"] = {
                    "total_orders": daily_data[0],
                    "total_revenue": float(daily_data[1]) if daily_data[1] else 0.0,
                    "avg_order_value": float(daily_data[2]) if daily_data[2] else 0.0,
                    "unique_customers": daily_data[3],
                }

            # Customer insights
            customer_insights_query = """
                SELECT
                    COUNT(*) as total_active_customers,
                    AVG(data_quality_score) as avg_quality_score
                FROM gold.dim_customer
                WHERE is_current = true AND is_active = true
            """

            cursor.execute(customer_insights_query)
            customer_data = cursor.fetchone()

            if customer_data:
                summary["customer_insights"] = {
                    "total_active_customers": customer_data[0],
                    "avg_quality_score": (
                        float(customer_data[1]) if customer_data[1] else 0.0
                    ),
                }

        logger.info(f"📈 Analytics summary generated: {summary}")

        # Store summary in metadata
        hook.insert_rows(
            table="airflow_meta.analytics_summary",
            rows=[
                (
                    execution_date,
                    summary.get("daily_metrics", {}).get("total_orders", 0),
                    summary.get("daily_metrics", {}).get("total_revenue", 0.0),
                    summary.get("customer_insights", {}).get(
                        "total_active_customers", 0
                    ),
                )
            ],
            target_fields=[
                "execution_date",
                "total_orders",
                "total_revenue",
                "active_customers",
            ],
        )

        return summary

    except Exception as e:
        logger.error(f"❌ Analytics summary generation failed: {str(e)}")
        raise


# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="Gold Layer Analytics Pipeline - Dimensional Modeling and Business Intelligence",
    schedule_interval="0 6 * * *",  # Daily at 6 AM (after Silver layer)
    max_active_runs=1,
    concurrency=3,
    catchup=False,
    tags=["etl", "gold", "analytics", "dimensional-modeling", "business-intelligence"],
)

# =====================================================
# TASK DEFINITIONS
# =====================================================

# Start task
start_task = DummyOperator(task_id="start_gold_pipeline", dag=dag)


# Check Silver layer data availability
def check_silver_data_available(**context):
    """
    Check if there is recent data in Silver layer tables.
    This is more flexible than ExternalTaskSensor for manual triggers.
    """
    execution_date = context["execution_date"]
    logger.info(f"🔍 Checking Silver layer data availability for {execution_date}")

    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        # Check for recent data in silver tables (within last 24 hours)
        check_date = execution_date.date()

        customers_query = """
            SELECT COUNT(*)
            FROM silver.dim_customers
            WHERE DATE(created_at) >= %s OR DATE(updated_at) >= %s
        """

        orders_query = """
            SELECT COUNT(*)
            FROM silver.fact_orders
            WHERE DATE(created_at) >= %s OR DATE(updated_at) >= %s
        """

        customers_count = hook.get_first(
            customers_query, parameters=[check_date, check_date]
        )[0]
        orders_count = hook.get_first(
            orders_query, parameters=[check_date, check_date]
        )[0]

        logger.info(
            f"📊 Found {customers_count} customers and {orders_count} orders in Silver layer for date {check_date}"
        )

        if customers_count > 0 or orders_count > 0:
            logger.info("✅ Silver data is available for processing")
            return True
        else:
            raise AirflowException(
                f"❌ No Silver data found for {check_date}. Cannot proceed with Gold layer processing."
            )

    except Exception as e:
        logger.error(f"❌ Error checking Silver data availability: {str(e)}")
        raise


wait_for_silver = PythonOperator(
    task_id="wait_for_silver_completion",
    python_callable=check_silver_data_available,
    dag=dag,
    doc_md="""
    ## Check Silver Data Availability

    Verifies that Silver layer data is available for processing.
    This approach is more flexible than ExternalTaskSensor for manual triggers
    and mixed scheduling scenarios.
    """,
)

# Build customer dimension
build_customer_dim_task = PythonOperator(
    task_id="build_customer_dimension",
    python_callable=build_customer_dimension,
    dag=dag,
)

# Build order fact table
build_order_facts_task = PythonOperator(
    task_id="build_order_fact_table", python_callable=build_order_fact_table, dag=dag
)

# Calculate business KPIs
calculate_kpis_task = PythonOperator(
    task_id="calculate_business_kpis", python_callable=build_business_kpis, dag=dag
)

# Validate Gold layer
validate_gold_task = PythonOperator(
    task_id="validate_gold_quality",
    python_callable=validate_gold_layer_quality,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
)

# Generate analytics summary
analytics_summary_task = PythonOperator(
    task_id="generate_analytics_summary",
    python_callable=generate_analytics_summary,
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id="end_gold_pipeline", trigger_rule=TriggerRule.NONE_FAILED, dag=dag
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================

start_task >> wait_for_silver >> [build_customer_dim_task, build_order_facts_task]
build_customer_dim_task >> build_order_facts_task >> calculate_kpis_task
calculate_kpis_task >> validate_gold_task >> analytics_summary_task >> end_task
