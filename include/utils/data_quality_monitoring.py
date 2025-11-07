"""
MetaLayer Real-time Data Quality Monitoring Framework
====================================================

Advanced data quality monitoring using Great Expectations with:
- Real-time data validation and alerting
- Automated expectation suite generation
- Quality dashboard with Grafana integration
- Comprehensive data profiling and anomaly detection

Features:
- Great Expectations integration for data validation
- Real-time quality metrics collection
- Automated anomaly detection
- Quality score calculation and trending
- Integration with monitoring stack (Prometheus/Grafana)
"""

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

# Great Expectations imports
try:
    import great_expectations as gx
    from great_expectations.checkpoint import SimpleCheckpoint
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.data_context.types.base import DataContextConfig
    from great_expectations.exceptions import DataContextError

    GX_AVAILABLE = True
except ImportError:
    logging.warning(
        "Great Expectations not available. Install with: pip install great_expectations"
    )
    GX_AVAILABLE = False

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


@dataclass
class DataQualityMetric:
    """Data quality metric structure"""

    metric_name: str
    table_name: str
    column_name: Optional[str]
    metric_value: Union[float, int, bool]
    threshold_value: Optional[Union[float, int]]
    status: str  # 'pass', 'fail', 'warning'
    timestamp: datetime
    details: Optional[Dict[str, Any]] = None


@dataclass
class QualityCheckResult:
    """Result of a quality check execution"""

    check_id: str
    table_name: str
    total_expectations: int
    successful_expectations: int
    failed_expectations: int
    success_rate: float
    execution_time: float
    timestamp: datetime
    metrics: List[DataQualityMetric]
    summary: Dict[str, Any]


class DataQualityMonitor:
    """Real-time data quality monitoring with Great Expectations"""

    def __init__(
        self,
        conn_id: str = "postgres_default",
        context_root_dir: str = "/opt/airflow/data_quality",
    ):
        self.conn_id = conn_id
        self.context_root_dir = Path(context_root_dir)
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.context = None
        self.metrics_table = "airflow_meta.data_quality_metrics"
        self.results_table = "airflow_meta.data_quality_results"

        if GX_AVAILABLE:
            self._initialize_great_expectations()
        else:
            logger.warning(
                "Great Expectations not available - using basic quality checks"
            )

        self._ensure_monitoring_tables()

    def _initialize_great_expectations(self):
        """Initialize Great Expectations data context"""
        try:
            # Create directory structure
            self.context_root_dir.mkdir(parents=True, exist_ok=True)

            # Configure data context
            data_context_config = DataContextConfig(
                config_version=3,
                datasources={
                    "postgres_datasource": {
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": "SqlAlchemyExecutionEngine",
                            "connection_string": self.hook.get_uri(),
                        },
                        "data_connectors": {
                            "default_runtime_data_connector": {
                                "class_name": "RuntimeDataConnector",
                                "batch_identifiers": ["default_identifier_name"],
                            }
                        },
                    }
                },
                stores={
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                self.context_root_dir / "expectations"
                            ),
                        },
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                self.context_root_dir / "validations"
                            ),
                        },
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore"
                    },
                    "checkpoint_store": {
                        "class_name": "CheckpointStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(
                                self.context_root_dir / "checkpoints"
                            ),
                        },
                    },
                },
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                checkpoint_store_name="checkpoint_store",
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": str(self.context_root_dir / "data_docs"),
                        },
                        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
                    }
                },
            )

            # Initialize context
            self.context = gx.data_context.BaseDataContext(
                project_config=data_context_config
            )
            logger.info("Great Expectations context initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations: {e}")
            self.context = None

    def _ensure_monitoring_tables(self):
        """Create data quality monitoring tables"""
        create_schema_sql = "CREATE SCHEMA IF NOT EXISTS airflow_meta;"

        create_metrics_table = f"""
        CREATE TABLE IF NOT EXISTS {self.metrics_table} (
            id SERIAL PRIMARY KEY,
            check_id VARCHAR(100) NOT NULL,
            metric_name VARCHAR(200) NOT NULL,
            table_name VARCHAR(200) NOT NULL,
            column_name VARCHAR(200),
            metric_value DECIMAL(18,6),
            threshold_value DECIMAL(18,6),
            status VARCHAR(20) NOT NULL,
            details JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_dq_metrics_table_time
        ON {self.metrics_table}(table_name, timestamp);

        CREATE INDEX IF NOT EXISTS idx_dq_metrics_status
        ON {self.metrics_table}(status, timestamp);
        """

        create_results_table = f"""
        CREATE TABLE IF NOT EXISTS {self.results_table} (
            id SERIAL PRIMARY KEY,
            check_id VARCHAR(100) UNIQUE NOT NULL,
            table_name VARCHAR(200) NOT NULL,
            total_expectations INTEGER NOT NULL,
            successful_expectations INTEGER NOT NULL,
            failed_expectations INTEGER NOT NULL,
            success_rate DECIMAL(5,2) NOT NULL,
            execution_time DECIMAL(10,3) NOT NULL,
            summary JSONB,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_dq_results_table_time
        ON {self.results_table}(table_name, timestamp);
        """

        try:
            self.hook.run(
                [create_schema_sql, create_metrics_table, create_results_table]
            )
            logger.info("Data quality monitoring tables initialized")
        except Exception as e:
            logger.error(f"Failed to create monitoring tables: {e}")
            raise AirflowException(f"Monitoring table creation failed: {e}")

    def create_expectation_suite(
        self, table_name: str, schema_name: str = "bronze"
    ) -> Dict[str, Any]:
        """Auto-generate expectation suite based on data profiling"""
        if not GX_AVAILABLE or not self.context:
            logger.warning("Great Expectations not available - skipping suite creation")
            return {}

        try:
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="postgres_datasource",
                data_connector_name="default_runtime_data_connector",
                data_asset_name=f"{schema_name}_{table_name}",
                runtime_parameters={
                    "query": f"SELECT * FROM {schema_name}.{table_name} LIMIT 10000"
                },
                batch_identifiers={
                    "default_identifier_name": f"{schema_name}_{table_name}"
                },
            )

            # Get validator
            expectation_suite_name = f"{schema_name}_{table_name}_suite"
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=expectation_suite_name,
            )

            # Auto-generate basic expectations

            # 1. Table-level expectations
            validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)

            # 2. Get column info
            columns = validator.get_dataset().columns

            for column in columns:
                column_data = validator.get_dataset()[column]

                # Basic column expectations
                validator.expect_column_to_exist(column)

                # Null value expectations
                null_percentage = column_data.isnull().mean() * 100
                if null_percentage < 10:  # If less than 10% nulls, expect low null rate
                    validator.expect_column_values_to_not_be_null(column)

                # Data type specific expectations
                if pd.api.types.is_numeric_dtype(column_data):
                    # Numeric columns
                    min_val = column_data.min()
                    max_val = column_data.max()
                    validator.expect_column_values_to_be_between(
                        column, min_value=min_val, max_value=max_val, mostly=0.95
                    )

                elif pd.api.types.is_string_dtype(column_data):
                    # String columns
                    if "email" in column.lower():
                        validator.expect_column_values_to_match_regex(
                            column,
                            regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                            mostly=0.9,
                        )

                    # Length expectations for string columns
                    max_length = column_data.str.len().max()
                    if pd.notna(max_length):
                        validator.expect_column_value_lengths_to_be_between(
                            column,
                            min_value=1,
                            max_value=int(max_length * 1.2),  # Allow 20% buffer
                            mostly=0.95,
                        )

                elif pd.api.types.is_datetime64_any_dtype(column_data):
                    # Date columns
                    min_date = column_data.min()
                    max_date = column_data.max()
                    if pd.notna(min_date) and pd.notna(max_date):
                        validator.expect_column_values_to_be_between(
                            column,
                            min_value=min_date,
                            max_value=datetime.now()
                            + timedelta(days=365),  # Allow future dates up to 1 year
                            mostly=0.99,
                        )

            # Save expectation suite
            validator.save_expectation_suite(discard_failed_expectations=False)

            suite_info = {
                "suite_name": expectation_suite_name,
                "total_expectations": len(
                    validator.get_expectation_suite().expectations
                ),
                "columns_analyzed": len(columns),
                "created_at": datetime.now().isoformat(),
            }

            logger.info(
                f"Created expectation suite for {schema_name}.{table_name}: {suite_info}"
            )
            return suite_info

        except Exception as e:
            logger.error(f"Failed to create expectation suite for {table_name}: {e}")
            return {"error": str(e)}

    def run_quality_check(
        self,
        table_name: str,
        schema_name: str = "bronze",
        expectation_suite_name: Optional[str] = None,
    ) -> QualityCheckResult:
        """Run comprehensive data quality check"""
        start_time = datetime.now()
        check_id = str(uuid.uuid4())

        if not expectation_suite_name:
            expectation_suite_name = f"{schema_name}_{table_name}_suite"

        try:
            if GX_AVAILABLE and self.context:
                result = self._run_great_expectations_check(
                    table_name, schema_name, expectation_suite_name, check_id
                )
            else:
                result = self._run_basic_quality_check(
                    table_name, schema_name, check_id
                )

            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()
            result.execution_time = execution_time

            # Store results
            self._store_quality_results(result)

            return result

        except Exception as e:
            logger.error(f"Quality check failed for {table_name}: {e}")

            # Return error result
            return QualityCheckResult(
                check_id=check_id,
                table_name=f"{schema_name}.{table_name}",
                total_expectations=0,
                successful_expectations=0,
                failed_expectations=1,
                success_rate=0.0,
                execution_time=(datetime.now() - start_time).total_seconds(),
                timestamp=datetime.now(),
                metrics=[],
                summary={"error": str(e)},
            )

    def _run_great_expectations_check(
        self,
        table_name: str,
        schema_name: str,
        expectation_suite_name: str,
        check_id: str,
    ) -> QualityCheckResult:
        """Run quality check using Great Expectations"""
        try:
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="postgres_datasource",
                data_connector_name="default_runtime_data_connector",
                data_asset_name=f"{schema_name}_{table_name}",
                runtime_parameters={
                    "query": f"SELECT * FROM {schema_name}.{table_name}"
                },
                batch_identifiers={
                    "default_identifier_name": f"{schema_name}_{table_name}"
                },
            )

            # Create and run checkpoint
            checkpoint_config = {
                "name": f"{table_name}_checkpoint",
                "config_version": 1.0,
                "template_name": None,
                "module_name": "great_expectations.checkpoint",
                "class_name": "SimpleCheckpoint",
                "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
                "expectation_suite_name": expectation_suite_name,
                "batch_request": batch_request,
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    }
                ],
                "evaluation_parameters": {},
                "runtime_configuration": {},
                "validations": [
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": expectation_suite_name,
                    }
                ],
            }

            # Run checkpoint
            checkpoint_result = self.context.run_checkpoint(**checkpoint_config)

            # Parse results
            validation_result = checkpoint_result.list_validation_results()[0]

            # Extract metrics
            metrics = []
            for expectation_result in validation_result.results:
                metric = DataQualityMetric(
                    metric_name=expectation_result.expectation_config.expectation_type,
                    table_name=f"{schema_name}.{table_name}",
                    column_name=expectation_result.expectation_config.kwargs.get(
                        "column"
                    ),
                    metric_value=1.0 if expectation_result.success else 0.0,
                    threshold_value=None,
                    status="pass" if expectation_result.success else "fail",
                    timestamp=datetime.now(),
                    details=expectation_result.result,
                )
                metrics.append(metric)

            # Calculate summary statistics
            total_expectations = len(validation_result.results)
            successful_expectations = sum(
                1 for r in validation_result.results if r.success
            )
            failed_expectations = total_expectations - successful_expectations
            success_rate = (
                (successful_expectations / total_expectations * 100)
                if total_expectations > 0
                else 0
            )

            return QualityCheckResult(
                check_id=check_id,
                table_name=f"{schema_name}.{table_name}",
                total_expectations=total_expectations,
                successful_expectations=successful_expectations,
                failed_expectations=failed_expectations,
                success_rate=success_rate,
                execution_time=0.0,  # Will be set by caller
                timestamp=datetime.now(),
                metrics=metrics,
                summary={
                    "validation_success": validation_result.success,
                    "statistics": validation_result.statistics,
                    "meta": validation_result.meta,
                },
            )

        except Exception as e:
            logger.error(f"Great Expectations check failed: {e}")
            raise

    def _run_basic_quality_check(
        self, table_name: str, schema_name: str, check_id: str
    ) -> QualityCheckResult:
        """Run basic quality checks without Great Expectations"""
        try:
            metrics = []
            full_table_name = f"{schema_name}.{table_name}"

            # Basic checks
            primary_key = self._get_primary_key(schema_name, table_name)
            checks = [
                (
                    "row_count",
                    f"SELECT COUNT(*) FROM {full_table_name}",
                    lambda x: x > 0,
                ),
                (
                    "null_check",
                    f"SELECT COUNT(*) FROM {full_table_name} WHERE {primary_key} IS NULL",
                    lambda x: x == 0,
                ),
                (
                    "duplicate_check",
                    f"SELECT COUNT(*) - COUNT(DISTINCT {primary_key}) FROM {full_table_name}",
                    lambda x: x == 0,
                ),
            ]

            successful = 0
            total = len(checks)

            for check_name, query, validation_func in checks:
                try:
                    result = self.hook.get_first(query)[0]
                    is_valid = validation_func(result)

                    metric = DataQualityMetric(
                        metric_name=check_name,
                        table_name=full_table_name,
                        column_name=None,
                        metric_value=result,
                        threshold_value=None,
                        status="pass" if is_valid else "fail",
                        timestamp=datetime.now(),
                    )
                    metrics.append(metric)

                    if is_valid:
                        successful += 1

                except Exception as e:
                    logger.error(f"Basic check {check_name} failed: {e}")
                    metric = DataQualityMetric(
                        metric_name=check_name,
                        table_name=full_table_name,
                        column_name=None,
                        metric_value=0,
                        threshold_value=None,
                        status="fail",
                        timestamp=datetime.now(),
                        details={"error": str(e)},
                    )
                    metrics.append(metric)

            success_rate = (successful / total * 100) if total > 0 else 0

            return QualityCheckResult(
                check_id=check_id,
                table_name=full_table_name,
                total_expectations=total,
                successful_expectations=successful,
                failed_expectations=total - successful,
                success_rate=success_rate,
                execution_time=0.0,  # Will be set by caller
                timestamp=datetime.now(),
                metrics=metrics,
                summary={"check_type": "basic_validation"},
            )

        except Exception as e:
            logger.error(f"Basic quality check failed: {e}")
            raise

    def _get_primary_key(self, schema_name: str, table_name: str) -> str:
        """Get primary key column for a table"""
        try:
            pk_query = """
            SELECT column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s
                AND tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            LIMIT 1
            """

            result = self.hook.get_first(pk_query, parameters=(schema_name, table_name))
            return result[0] if result else "id"  # Default to 'id' if no PK found

        except Exception:
            return "id"  # Fallback

    def _store_quality_results(self, result: QualityCheckResult):
        """Store quality check results in database"""
        try:
            # Store main result
            result_sql = f"""
            INSERT INTO {self.results_table}
                (check_id, table_name, total_expectations, successful_expectations,
                 failed_expectations, success_rate, execution_time, summary, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (check_id) DO UPDATE SET
                total_expectations = EXCLUDED.total_expectations,
                successful_expectations = EXCLUDED.successful_expectations,
                failed_expectations = EXCLUDED.failed_expectations,
                success_rate = EXCLUDED.success_rate,
                execution_time = EXCLUDED.execution_time,
                summary = EXCLUDED.summary,
                timestamp = EXCLUDED.timestamp
            """

            self.hook.run(
                result_sql,
                parameters=(
                    result.check_id,
                    result.table_name,
                    result.total_expectations,
                    result.successful_expectations,
                    result.failed_expectations,
                    result.success_rate,
                    result.execution_time,
                    json.dumps(result.summary),
                    result.timestamp,
                ),
            )

            # Store individual metrics
            for metric in result.metrics:
                metric_sql = f"""
                INSERT INTO {self.metrics_table}
                    (check_id, metric_name, table_name, column_name, metric_value,
                     threshold_value, status, details, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                self.hook.run(
                    metric_sql,
                    parameters=(
                        result.check_id,
                        metric.metric_name,
                        metric.table_name,
                        metric.column_name,
                        metric.metric_value,
                        metric.threshold_value,
                        metric.status,
                        json.dumps(metric.details) if metric.details else None,
                        metric.timestamp,
                    ),
                )

            logger.info(f"Stored quality results for check {result.check_id}")

        except Exception as e:
            logger.error(f"Failed to store quality results: {e}")

    def get_quality_dashboard_data(self, days_back: int = 7) -> Dict[str, Any]:
        """Get data for quality monitoring dashboard"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_back)

            # Overall quality trends
            trend_query = f"""
            SELECT
                DATE(timestamp) as date,
                AVG(success_rate) as avg_success_rate,
                COUNT(*) as total_checks,
                SUM(CASE WHEN success_rate < 95 THEN 1 ELSE 0 END) as failing_checks
            FROM {self.results_table}
            WHERE timestamp >= %s
            GROUP BY DATE(timestamp)
            ORDER BY date DESC
            """

            trends = self.hook.get_records(trend_query, parameters=(cutoff_date,))

            # Table-level quality summary
            table_summary_query = f"""
            SELECT
                table_name,
                AVG(success_rate) as avg_success_rate,
                MIN(success_rate) as min_success_rate,
                MAX(success_rate) as max_success_rate,
                COUNT(*) as total_runs,
                MAX(timestamp) as last_check
            FROM {self.results_table}
            WHERE timestamp >= %s
            GROUP BY table_name
            ORDER BY avg_success_rate ASC
            """

            table_summary = self.hook.get_records(
                table_summary_query, parameters=(cutoff_date,)
            )

            # Failed metrics breakdown
            failed_metrics_query = f"""
            SELECT
                metric_name,
                table_name,
                COUNT(*) as failure_count,
                MAX(timestamp) as last_failure
            FROM {self.metrics_table}
            WHERE status = 'fail' AND timestamp >= %s
            GROUP BY metric_name, table_name
            ORDER BY failure_count DESC
            LIMIT 10
            """

            failed_metrics = self.hook.get_records(
                failed_metrics_query, parameters=(cutoff_date,)
            )

            return {
                "quality_trends": [
                    {
                        "date": str(row[0]),
                        "avg_success_rate": float(row[1]) if row[1] else 0,
                        "total_checks": row[2],
                        "failing_checks": row[3],
                    }
                    for row in trends
                ],
                "table_summary": [
                    {
                        "table_name": row[0],
                        "avg_success_rate": float(row[1]) if row[1] else 0,
                        "min_success_rate": float(row[2]) if row[2] else 0,
                        "max_success_rate": float(row[3]) if row[3] else 0,
                        "total_runs": row[4],
                        "last_check": row[5].isoformat() if row[5] else None,
                    }
                    for row in table_summary
                ],
                "failed_metrics": [
                    {
                        "metric_name": row[0],
                        "table_name": row[1],
                        "failure_count": row[2],
                        "last_failure": row[3].isoformat() if row[3] else None,
                    }
                    for row in failed_metrics
                ],
                "summary": {
                    "total_tables": len(table_summary),
                    "avg_quality_score": (
                        sum(row[1] for row in table_summary if row[1])
                        / len(table_summary)
                        if table_summary
                        else 0
                    ),
                    "tables_below_threshold": len(
                        [row for row in table_summary if row[1] and row[1] < 95]
                    ),
                    "generated_at": datetime.now().isoformat(),
                },
            }

        except Exception as e:
            logger.error(f"Failed to get dashboard data: {e}")
            return {"error": str(e)}


def basic_data_quality_check(
    table_name: str, schema_name: str = "bronze", conn_id: str = "postgres_default"
) -> Dict[str, Any]:
    """
    Basic data quality check when Great Expectations is not available
    Performs essential data validation using SQL queries
    """
    hook = PostgresHook(postgres_conn_id=conn_id)

    try:
        full_table_name = f"{schema_name}.{table_name}"

        # Basic metrics
        record_count = hook.get_first(f"SELECT COUNT(*) FROM {full_table_name}")[0]

        # Get table columns
        columns_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        columns = hook.get_records(columns_query)

        metrics = []
        issues = []

        for column_name, data_type, is_nullable in columns:
            # Check for null values
            null_count = hook.get_first(
                f"""
                SELECT COUNT(*) FROM {full_table_name}
                WHERE {column_name} IS NULL
            """
            )[0]

            null_percentage = (
                (null_count / record_count * 100) if record_count > 0 else 0
            )

            metrics.append(
                {
                    "column": column_name,
                    "data_type": data_type,
                    "null_count": null_count,
                    "null_percentage": round(null_percentage, 2),
                }
            )

            # Flag potential issues
            if is_nullable == "NO" and null_count > 0:
                issues.append(
                    f"Column {column_name} has {null_count} null values but is NOT NULL"
                )

            if null_percentage > 50:
                issues.append(
                    f"Column {column_name} has high null percentage: {null_percentage}%"
                )

        # Additional table-specific checks
        if "email" in [col[0] for col in columns]:
            invalid_emails = hook.get_first(
                f"""
                SELECT COUNT(*) FROM {full_table_name}
                WHERE email IS NOT NULL AND email NOT LIKE '%@%'
            """
            )[0]

            if invalid_emails > 0:
                issues.append(f"Found {invalid_emails} invalid email formats")

        # Calculate quality score
        quality_score = max(0, 100 - len(issues) * 10)

        return {
            "table_name": full_table_name,
            "record_count": record_count,
            "column_count": len(columns),
            "column_metrics": metrics,
            "quality_issues": issues,
            "quality_score": quality_score,
            "check_type": "basic_sql_validation",
            "timestamp": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Basic quality check failed for {full_table_name}: {e}")
        return {
            "table_name": f"{schema_name}.{table_name}",
            "error": str(e),
            "quality_score": 0,
            "check_type": "basic_sql_validation",
            "timestamp": datetime.now().isoformat(),
        }


# Factory functions for DAG integration
def create_quality_monitor(conn_id: str = "postgres_default") -> DataQualityMonitor:
    """Create data quality monitor instance"""
    return DataQualityMonitor(conn_id=conn_id)


def run_table_quality_check(
    table_name: str, schema_name: str = "bronze", conn_id: str = "postgres_default"
) -> Dict[str, Any]:
    """Run quality check for a specific table"""
    if not GX_AVAILABLE:
        # Use basic fallback when Great Expectations is not available
        logger.info(
            "Using basic SQL-based quality checks (Great Expectations not available)"
        )
        return basic_data_quality_check(table_name, schema_name, conn_id)

    try:
        monitor = create_quality_monitor(conn_id)

        # Create expectation suite if it doesn't exist
        monitor.create_expectation_suite(table_name, schema_name)

        # Run quality check
        result = monitor.run_quality_check(table_name, schema_name)

        return asdict(result)
    except Exception as e:
        logger.warning(
            f"Great Expectations check failed, falling back to basic checks: {e}"
        )
        return basic_data_quality_check(table_name, schema_name, conn_id)


def get_quality_dashboard() -> Dict[str, Any]:
    """Get quality dashboard data"""
    monitor = create_quality_monitor()
    return monitor.get_quality_dashboard_data()


class DataQualityValidator:
    """Simple data quality validator for ETL pipeline validation"""

    def __init__(self, conn_id: str = "postgres_default"):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        logger.info(f"DataQualityValidator initialized with connection: {conn_id}")

    def validate_table_completeness(
        self, table_name: str, schema_name: str = "bronze"
    ) -> Dict[str, Any]:
        """
        Validate table completeness (no null values in key fields)

        Args:
            table_name: Name of the table to validate
            schema_name: Schema name (default: bronze)

        Returns:
            Dict with validation results
        """
        try:
            # Check for null values in key fields
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_ids,
                COUNT(CASE WHEN created_at IS NULL THEN 1 END) as null_created_dates
            FROM {schema_name}.{table_name}
            """

            result = self.hook.get_first(query)

            total_rows = result[0] if result else 0
            null_customer_ids = result[1] if result and len(result) > 1 else 0
            null_created_dates = result[2] if result and len(result) > 2 else 0

            completeness_score = 1.0
            if total_rows > 0:
                completeness_score = max(
                    0.0,
                    1.0 - (null_customer_ids + null_created_dates) / (total_rows * 2),
                )

            return {
                "table_name": f"{schema_name}.{table_name}",
                "total_rows": total_rows,
                "null_customer_ids": null_customer_ids,
                "null_created_dates": null_created_dates,
                "completeness_score": round(completeness_score, 3),
                "validation_time": datetime.now().isoformat(),
                "status": "PASSED" if completeness_score >= 0.95 else "FAILED",
            }

        except Exception as e:
            logger.error(
                f"Completeness validation failed for {schema_name}.{table_name}: {e}"
            )
            return {
                "table_name": f"{schema_name}.{table_name}",
                "error": str(e),
                "status": "ERROR",
                "validation_time": datetime.now().isoformat(),
            }

    def validate_data_freshness(
        self,
        table_name: str,
        schema_name: str = "bronze",
        date_column: str = "created_at",
        max_age_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Validate data freshness (recent data available)

        Args:
            table_name: Name of the table to validate
            schema_name: Schema name (default: bronze)
            date_column: Column name for date validation
            max_age_hours: Maximum age of data in hours

        Returns:
            Dict with validation results
        """
        try:
            query = f"""
            SELECT
                MAX({date_column}) as latest_record,
                COUNT(*) as total_rows,
                COUNT(CASE WHEN {date_column} >= NOW() - INTERVAL '{max_age_hours} hours' THEN 1 END) as recent_rows
            FROM {schema_name}.{table_name}
            WHERE {date_column} IS NOT NULL
            """

            result = self.hook.get_first(query)

            if not result or not result[0]:
                return {
                    "table_name": f"{schema_name}.{table_name}",
                    "error": "No data found or invalid date column",
                    "status": "FAILED",
                    "validation_time": datetime.now().isoformat(),
                }

            latest_record = result[0]
            total_rows = result[1] if result[1] else 0
            recent_rows = result[2] if result[2] else 0

            # Calculate time since latest record
            now = datetime.now()
            if isinstance(latest_record, str):
                latest_record = datetime.fromisoformat(
                    latest_record.replace("Z", "+00:00")
                )

            hours_since_latest = (
                now - latest_record.replace(tzinfo=None)
            ).total_seconds() / 3600

            freshness_score = max(0.0, 1.0 - (hours_since_latest / max_age_hours))

            return {
                "table_name": f"{schema_name}.{table_name}",
                "latest_record": latest_record.isoformat(),
                "hours_since_latest": round(hours_since_latest, 2),
                "total_rows": total_rows,
                "recent_rows": recent_rows,
                "freshness_score": round(freshness_score, 3),
                "validation_time": datetime.now().isoformat(),
                "status": "PASSED" if hours_since_latest <= max_age_hours else "FAILED",
            }

        except Exception as e:
            logger.error(
                f"Freshness validation failed for {schema_name}.{table_name}: {e}"
            )
            return {
                "table_name": f"{schema_name}.{table_name}",
                "error": str(e),
                "status": "ERROR",
                "validation_time": datetime.now().isoformat(),
            }

    def validate_data_quality(
        self, table_name: str, schema_name: str = "bronze"
    ) -> Dict[str, Any]:
        """
        Comprehensive data quality validation

        Args:
            table_name: Name of the table to validate
            schema_name: Schema name (default: bronze)

        Returns:
            Dict with comprehensive validation results
        """
        try:
            logger.info(
                f"Running data quality validation for {schema_name}.{table_name}"
            )

            # Run completeness check
            completeness_result = self.validate_table_completeness(
                table_name, schema_name
            )

            # Run freshness check
            freshness_result = self.validate_data_freshness(table_name, schema_name)

            # Calculate overall quality score
            completeness_score = completeness_result.get("completeness_score", 0.0)
            freshness_score = freshness_result.get("freshness_score", 0.0)

            overall_score = (completeness_score + freshness_score) / 2
            overall_status = "PASSED" if overall_score >= 0.8 else "FAILED"

            return {
                "table_name": f"{schema_name}.{table_name}",
                "overall_score": round(overall_score, 3),
                "overall_status": overall_status,
                "completeness": completeness_result,
                "freshness": freshness_result,
                "validation_time": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(
                f"Data quality validation failed for {schema_name}.{table_name}: {e}"
            )
            return {
                "table_name": f"{schema_name}.{table_name}",
                "error": str(e),
                "status": "ERROR",
                "validation_time": datetime.now().isoformat(),
            }
