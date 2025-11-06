#!/usr/bin/env python3
"""
AIRFLOW CONNECTIONS AND VARIABLES SETUP FOR METALAYER ETL PIPELINE
=================================================================

This script automatically configures all necessary Airflow connections,
variables, and configurations required for the MetaLayer ETL pipeline.

Features:
- Database connections (PostgreSQL, Redis)
- Email/SMTP configurations
- Pipeline variables and thresholds
- Security and authentication setup
- Performance and monitoring configurations

Usage:
    python setup_airflow_config.py

Requirements:
    - Airflow CLI available in PATH
    - Docker containers running
    - Administrative access to Airflow
"""

import os
import sys
import subprocess
import json
import logging
from typing import Dict, List, Any
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# CONFIGURATION CONSTANTS
# =====================================================

# Database connection details
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "database": "airflow",
    "username": "airflow",
    "password": "airflow_password",  # Should be changed in production
}

# Redis configuration
REDIS_CONFIG = {
    "host": "redis",
    "port": "6379",
    "password": "redis_password",  # Should be changed in production
    "database": "0",
}

# SMTP configuration
SMTP_CONFIG = {
    "host": "localhost",
    "port": "587",
    "username": "",
    "password": "",
    "from_email": "airflow@metalayer.com",
}

# =====================================================
# CONNECTION DEFINITIONS
# =====================================================

AIRFLOW_CONNECTIONS = [
    {
        "conn_id": "postgres_default",
        "conn_type": "postgres",
        "description": "PostgreSQL connection for MetaLayer ETL pipeline",
        "host": POSTGRES_CONFIG["host"],
        "port": POSTGRES_CONFIG["port"],
        "login": POSTGRES_CONFIG["username"],
        "password": POSTGRES_CONFIG["password"],
        "schema": POSTGRES_CONFIG["database"],
        "extra": json.dumps(
            {
                "sslmode": "disable",
                "connect_timeout": "10",
                "application_name": "metalayer_etl",
            }
        ),
    },
    {
        "conn_id": "postgres_metalayer",
        "conn_type": "postgres",
        "description": "PostgreSQL connection for MetaLayer data schemas",
        "host": POSTGRES_CONFIG["host"],
        "port": POSTGRES_CONFIG["port"],
        "login": POSTGRES_CONFIG["username"],
        "password": POSTGRES_CONFIG["password"],
        "schema": "metalayer",
        "extra": json.dumps({"sslmode": "disable", "connect_timeout": "10"}),
    },
    {
        "conn_id": "redis_default",
        "conn_type": "redis",
        "description": "Redis connection for Celery message broker",
        "host": REDIS_CONFIG["host"],
        "port": REDIS_CONFIG["port"],
        "password": REDIS_CONFIG["password"],
        "extra": json.dumps(
            {
                "db": REDIS_CONFIG["database"],
                "socket_timeout": "30",
                "socket_connect_timeout": "30",
            }
        ),
    },
    {
        "conn_id": "smtp_default",
        "conn_type": "email",
        "description": "SMTP connection for email notifications",
        "host": SMTP_CONFIG["host"],
        "port": SMTP_CONFIG["port"],
        "login": SMTP_CONFIG["username"],
        "password": SMTP_CONFIG["password"],
        "extra": json.dumps(
            {
                "from_email": SMTP_CONFIG["from_email"],
                "smtp_starttls": True,
                "smtp_ssl": False,
            }
        ),
    },
]

# =====================================================
# VARIABLE DEFINITIONS
# =====================================================

AIRFLOW_VARIABLES = [
    # Pipeline configuration
    {
        "key": "pipeline_version",
        "value": "1.0.0",
        "description": "Current version of the MetaLayer ETL pipeline",
    },
    {
        "key": "environment",
        "value": "docker_development",
        "description": "Current deployment environment",
    },
    # Data processing configuration
    {
        "key": "bronze_batch_size",
        "value": "10000",
        "description": "Batch size for Bronze layer data ingestion",
    },
    {
        "key": "silver_batch_size",
        "value": "25000",
        "description": "Batch size for Silver layer data transformation",
    },
    {
        "key": "gold_batch_size",
        "value": "50000",
        "description": "Batch size for Gold layer analytics processing",
    },
    # Quality and validation thresholds
    {
        "key": "data_quality_threshold",
        "value": "0.95",
        "description": "Minimum data quality score threshold (0.0 - 1.0)",
    },
    {
        "key": "bronze_validation_threshold",
        "value": "0.80",
        "description": "Data validation threshold for Bronze layer",
    },
    {
        "key": "silver_transformation_threshold",
        "value": "0.90",
        "description": "Data transformation quality threshold for Silver layer",
    },
    # Processing timeouts and limits
    {
        "key": "bronze_max_processing_minutes",
        "value": "60",
        "description": "Maximum processing time for Bronze layer in minutes",
    },
    {
        "key": "silver_max_processing_minutes",
        "value": "120",
        "description": "Maximum processing time for Silver layer in minutes",
    },
    {
        "key": "gold_max_processing_minutes",
        "value": "180",
        "description": "Maximum processing time for Gold layer in minutes",
    },
    {
        "key": "pipeline_sla_minutes",
        "value": "240",
        "description": "End-to-end pipeline SLA in minutes",
    },
    # Data retention policies
    {
        "key": "bronze_retention_days",
        "value": "30",
        "description": "Data retention period for Bronze layer in days",
    },
    {
        "key": "silver_retention_days",
        "value": "365",
        "description": "Data retention period for Silver layer in days",
    },
    {
        "key": "gold_retention_days",
        "value": "2555",
        "description": "Data retention period for Gold layer in days (7 years)",
    },
    {
        "key": "log_retention_days",
        "value": "30",
        "description": "Log file retention period in days",
    },
    # Monitoring and alerting
    {
        "key": "alert_email",
        "value": "data-team@company.com",
        "description": "Email address for pipeline alerts and notifications",
    },
    {
        "key": "enable_performance_monitoring",
        "value": "true",
        "description": "Enable detailed performance monitoring and metrics",
    },
    {
        "key": "enable_data_lineage",
        "value": "true",
        "description": "Enable data lineage tracking and audit logging",
    },
    {
        "key": "prometheus_metrics_port",
        "value": "9090",
        "description": "Port for Prometheus metrics collection",
    },
    # Business logic configuration
    {
        "key": "customer_quality_min_score",
        "value": "0.50",
        "description": "Minimum quality score to mark customer as active",
    },
    {
        "key": "order_validation_min_score",
        "value": "0.75",
        "description": "Minimum quality score to mark order as valid",
    },
    {
        "key": "scd_lookback_days",
        "value": "7",
        "description": "Lookback period for SCD Type 2 processing in days",
    },
    # File and path configuration
    {
        "key": "source_data_path",
        "value": "/opt/airflow/data/bronze_src",
        "description": "Base path for source data files",
    },
    {
        "key": "archive_data_path",
        "value": "/opt/airflow/data/archive",
        "description": "Path for archived processed files",
    },
    {
        "key": "error_data_path",
        "value": "/opt/airflow/data/error",
        "description": "Path for files with processing errors",
    },
]

# =====================================================
# HELPER FUNCTIONS
# =====================================================


def run_airflow_command(command: List[str]) -> bool:
    """Run an Airflow CLI command and return success status"""
    try:
        full_command = ["airflow"] + command
        logger.info(f"Executing: {' '.join(full_command)}")

        result = subprocess.run(
            full_command, capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            logger.info(f"✅ Command succeeded: {' '.join(command)}")
            return True
        else:
            logger.error(f"❌ Command failed: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"⏱️ Command timed out: {' '.join(command)}")
        return False
    except Exception as e:
        logger.error(f"❌ Command execution error: {str(e)}")
        return False


def check_airflow_availability() -> bool:
    """Check if Airflow CLI is available and working"""
    try:
        result = subprocess.run(
            ["airflow", "version"], capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            logger.info(f"✅ Airflow CLI available: {result.stdout.strip()}")
            return True
        else:
            logger.error("❌ Airflow CLI not working properly")
            return False
    except Exception as e:
        logger.error(f"❌ Airflow CLI not available: {str(e)}")
        return False


def create_connections() -> int:
    """Create all required Airflow connections"""
    logger.info("🔗 Creating Airflow connections...")

    success_count = 0

    for conn in AIRFLOW_CONNECTIONS:
        # Build connection URI
        if conn["conn_type"] == "postgres":
            uri = (
                f"postgresql://{conn['login']}:{conn['password']}"
                f"@{conn['host']}:{conn['port']}/{conn['schema']}"
            )
        elif conn["conn_type"] == "redis":
            uri = f"redis://:{conn['password']}@{conn['host']}:{conn['port']}/0"
        elif conn["conn_type"] == "email":
            uri = f"smtp://{conn['login']}:{conn['password']}@{conn['host']}:{conn['port']}"
        else:
            uri = f"{conn['conn_type']}://{conn['host']}:{conn['port']}"

        # Create connection using CLI
        command = [
            "connections",
            "add",
            conn["conn_id"],
            "--conn-type",
            conn["conn_type"],
            "--conn-host",
            conn["host"],
            "--conn-port",
            str(conn["port"]),
            "--conn-login",
            conn.get("login", ""),
            "--conn-password",
            conn.get("password", ""),
            "--conn-schema",
            conn.get("schema", ""),
            "--conn-description",
            conn["description"],
        ]

        # Add extra parameters if present
        if "extra" in conn and conn["extra"]:
            command.extend(["--conn-extra", conn["extra"]])

        if run_airflow_command(command):
            success_count += 1
        else:
            logger.warning(f"⚠️ Failed to create connection: {conn['conn_id']}")

    logger.info(f"✅ Created {success_count}/{len(AIRFLOW_CONNECTIONS)} connections")
    return success_count


def create_variables() -> int:
    """Create all required Airflow variables"""
    logger.info("📊 Creating Airflow variables...")

    success_count = 0

    for var in AIRFLOW_VARIABLES:
        command = [
            "variables",
            "set",
            var["key"],
            var["value"],
            "--description",
            var.get("description", ""),
        ]

        if run_airflow_command(command):
            success_count += 1
        else:
            logger.warning(f"⚠️ Failed to create variable: {var['key']}")

    logger.info(f"✅ Created {success_count}/{len(AIRFLOW_VARIABLES)} variables")
    return success_count


def verify_configuration() -> Dict[str, Any]:
    """Verify that all configurations were created successfully"""
    logger.info("🔍 Verifying configuration...")

    verification_results = {
        "connections_verified": 0,
        "variables_verified": 0,
        "total_connections": len(AIRFLOW_CONNECTIONS),
        "total_variables": len(AIRFLOW_VARIABLES),
        "verification_success": False,
    }

    # Verify connections
    for conn in AIRFLOW_CONNECTIONS:
        command = ["connections", "get", conn["conn_id"]]
        if run_airflow_command(command):
            verification_results["connections_verified"] += 1

    # Verify variables
    for var in AIRFLOW_VARIABLES:
        command = ["variables", "get", var["key"]]
        if run_airflow_command(command):
            verification_results["variables_verified"] += 1

    # Calculate success
    verification_results["verification_success"] = (
        verification_results["connections_verified"]
        == verification_results["total_connections"]
        and verification_results["variables_verified"]
        == verification_results["total_variables"]
    )

    logger.info(f"📋 Verification Results:")
    logger.info(
        f"   Connections: {verification_results['connections_verified']}/{verification_results['total_connections']}"
    )
    logger.info(
        f"   Variables: {verification_results['variables_verified']}/{verification_results['total_variables']}"
    )
    logger.info(f"   Overall Success: {verification_results['verification_success']}")

    return verification_results


def initialize_database_schema() -> bool:
    """Initialize the database schema using the SQL file"""
    logger.info("🗄️ Initializing database schema...")

    sql_file_path = (
        Path(__file__).parent / "include" / "sql" / "complete_schema_docker.sql"
    )

    if not sql_file_path.exists():
        logger.error(f"❌ SQL schema file not found: {sql_file_path}")
        return False

    try:
        # Use psql to execute the schema creation
        command = [
            "docker",
            "exec",
            "-i",
            "metalayer-postgres-1",
            "psql",
            "-U",
            POSTGRES_CONFIG["username"],
            "-d",
            POSTGRES_CONFIG["database"],
            "-f",
            "/dev/stdin",
        ]

        with open(sql_file_path, "r") as sql_file:
            result = subprocess.run(
                command,
                input=sql_file.read(),
                text=True,
                capture_output=True,
                timeout=60,
            )

        if result.returncode == 0:
            logger.info("✅ Database schema initialized successfully")
            return True
        else:
            logger.error(f"❌ Database schema initialization failed: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"❌ Error initializing database schema: {str(e)}")
        return False


# =====================================================
# MAIN EXECUTION
# =====================================================


def main():
    """Main execution function"""
    logger.info("🚀 Starting MetaLayer Airflow Configuration Setup")
    logger.info("=" * 60)

    # Check prerequisites
    if not check_airflow_availability():
        logger.error(
            "❌ Airflow CLI not available. Please ensure Airflow is running and accessible."
        )
        sys.exit(1)

    try:
        # Initialize database schema
        logger.info("\n📋 Step 1: Database Schema Initialization")
        schema_success = initialize_database_schema()

        # Create connections
        logger.info("\n📋 Step 2: Creating Airflow Connections")
        connections_created = create_connections()

        # Create variables
        logger.info("\n📋 Step 3: Creating Airflow Variables")
        variables_created = create_variables()

        # Verify configuration
        logger.info("\n📋 Step 4: Verification")
        verification_results = verify_configuration()

        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 CONFIGURATION SETUP SUMMARY")
        logger.info("=" * 60)
        logger.info(
            f"Database Schema: {'✅ Success' if schema_success else '❌ Failed'}"
        )
        logger.info(
            f"Connections Created: {connections_created}/{len(AIRFLOW_CONNECTIONS)}"
        )
        logger.info(f"Variables Created: {variables_created}/{len(AIRFLOW_VARIABLES)}")
        logger.info(
            f"Overall Success: {'✅ Yes' if verification_results['verification_success'] and schema_success else '❌ No'}"
        )

        if verification_results["verification_success"] and schema_success:
            logger.info(
                "\n🎉 MetaLayer Airflow configuration setup completed successfully!"
            )
            logger.info("🔄 You can now run the ETL pipeline DAGs.")
            logger.info("🌐 Access Airflow UI at: http://localhost:8080")
            logger.info("📊 Access Grafana dashboard at: http://localhost:3000")
            sys.exit(0)
        else:
            logger.error("\n❌ Configuration setup completed with errors.")
            logger.error(
                "📝 Please check the logs and fix any issues before running the pipeline."
            )
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n⏹️ Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Unexpected error during setup: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
