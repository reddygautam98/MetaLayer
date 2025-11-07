"""
Data Quality Testing Suite for MetaLayer
Comprehensive tests for data validation and quality assurance
"""

import pytest
import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any


@pytest.fixture
def db_connection():
    """Module-level database connection fixture for testing.

    This fixture attempts to connect to the database URL taken from the
    `DATABASE_URL` environment variable and yields a psycopg2 connection.
    If the connection fails the tests are skipped (same behavior as before).
    """
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://test_user:test_password@localhost:5432/test_airflow",
    )

    try:
        conn = psycopg2.connect(database_url)
    except Exception as e:
        pytest.skip(f"Database connection failed: {e}")

    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


class TestDataQuality:
    """Data Quality Test Suite"""

    def test_bronze_layer_completeness(self, db_connection):
        """Test bronze layer data completeness"""
        cursor = db_connection.cursor()

        # Check if bronze tables exist and have data
        tables = ["bronze.erp_sales_raw", "bronze.crm_customers_raw"]

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                assert count > 0, f"Table {table} is empty"

                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE created_at IS NULL")
                null_timestamps = cursor.fetchone()[0]
                assert (
                    null_timestamps == 0
                ), f"Found {null_timestamps} records with null timestamps in {table}"

            except psycopg2.Error:
                pytest.skip(f"Table {table} does not exist")

    def test_silver_layer_data_quality(self, db_connection):
        """Test silver layer data quality and transformations"""
        cursor = db_connection.cursor()

        # Test sales data quality
        try:
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver.sales_cleaned 
                WHERE sale_amount_clean <= 0 OR quantity_clean <= 0
            """
            )
            invalid_sales = cursor.fetchone()[0]
            assert invalid_sales == 0, f"Found {invalid_sales} invalid sales records"

            # Test email validation in customer data
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver.customers_standardized
                WHERE email_clean NOT LIKE '%@%' OR email_clean IS NULL
            """
            )
            invalid_emails = cursor.fetchone()[0]
            assert (
                invalid_emails == 0
            ), f"Found {invalid_emails} invalid email addresses"

        except psycopg2.Error:
            pytest.skip("Silver layer tables do not exist")

    def test_gold_layer_integrity(self, db_connection):
        """Test gold layer dimensional model integrity"""
        cursor = db_connection.cursor()

        try:
            # Test foreign key integrity
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold.fact_sales f
                LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
                WHERE d.customer_key IS NULL
            """
            )
            orphaned_records = cursor.fetchone()[0]
            assert (
                orphaned_records == 0
            ), f"Found {orphaned_records} orphaned fact records"

            # Test data consistency
            cursor.execute(
                """
                SELECT COUNT(*) FROM gold.fact_sales
                WHERE total_amount IS NULL OR total_amount <= 0
            """
            )
            invalid_amounts = cursor.fetchone()[0]
            assert (
                invalid_amounts == 0
            ), f"Found {invalid_amounts} invalid amounts in fact table"

        except psycopg2.Error:
            pytest.skip("Gold layer tables do not exist")

    def test_data_freshness(self, db_connection):
        """Test that data is fresh and up-to-date"""
        cursor = db_connection.cursor()

        # Check data freshness (last 24 hours)
        freshness_threshold = datetime.now() - timedelta(hours=24)

        tables_to_check = [
            "bronze.erp_sales_raw",
            "bronze.crm_customers_raw",
            "silver.sales_cleaned",
            "silver.customers_standardized",
        ]

        for table in tables_to_check:
            try:
                cursor.execute(
                    f"""
                    SELECT MAX(processed_date_utc) FROM {table}
                    WHERE processed_date_utc IS NOT NULL
                """
                )
                result = cursor.fetchone()[0]

                if result:
                    assert (
                        result > freshness_threshold
                    ), f"Data in {table} is stale (older than 24 hours)"

            except psycopg2.Error:
                pytest.skip(f"Table {table} does not exist or has no timestamp column")

    def test_duplicate_detection(self, db_connection):
        """Test for duplicate records in key tables"""
        cursor = db_connection.cursor()

        duplicate_checks = {
            "bronze.erp_sales_raw": "sales_id",
            "bronze.crm_customers_raw": "customer_id",
            "silver.sales_cleaned": "sales_id",
            "silver.customers_standardized": "customer_id",
            "gold.dim_customer": "customer_id",
        }

        for table, key_column in duplicate_checks.items():
            try:
                cursor.execute(
                    f"""
                    SELECT {key_column}, COUNT(*)
                    FROM {table}
                    GROUP BY {key_column}
                    HAVING COUNT(*) > 1
                    LIMIT 1
                """
                )

                duplicate = cursor.fetchone()
                assert (
                    duplicate is None
                ), f"Found duplicate {key_column} in {table}: {duplicate[0] if duplicate else 'None'}"

            except psycopg2.Error:
                pytest.skip(f"Table {table} does not exist")

    def test_business_rules_validation(self, db_connection):
        """Test business rules and constraints"""
        cursor = db_connection.cursor()

        try:
            # Test sales amount ranges
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver.sales_cleaned
                WHERE sale_amount_clean > 100000  -- Unusually large sales
            """
            )
            large_sales = cursor.fetchone()[0]

            # Allow some large sales, but flag if too many
            total_sales_cursor = db_connection.cursor()
            total_sales_cursor.execute("SELECT COUNT(*) FROM silver.sales_cleaned")
            total_sales = total_sales_cursor.fetchone()[0]

            if total_sales > 0:
                large_sales_percentage = (large_sales / total_sales) * 100
                assert (
                    large_sales_percentage < 5
                ), f"Too many unusually large sales: {large_sales_percentage:.2f}%"

            # Test customer age validation
            cursor.execute(
                """
                SELECT COUNT(*) FROM silver.customers_standardized
                WHERE dob_clean > CURRENT_DATE - INTERVAL '18 years'
                OR dob_clean < CURRENT_DATE - INTERVAL '120 years'
            """
            )
            invalid_ages = cursor.fetchone()[0]
            assert (
                invalid_ages == 0
            ), f"Found {invalid_ages} customers with invalid ages"

        except psycopg2.Error:
            pytest.skip("Business rule validation tables do not exist")


class TestDataProfiling:
    """Data profiling and statistics tests"""

    def test_null_value_thresholds(self, db_connection):
        """Test that null values are within acceptable thresholds"""
        cursor = db_connection.cursor()

        null_threshold = 5.0  # 5% null values threshold

        critical_columns = {
            "bronze.erp_sales_raw": ["sales_id", "customer_id", "sale_amount"],
            "bronze.crm_customers_raw": ["customer_id", "email"],
            "silver.sales_cleaned": ["sales_id", "customer_id", "sale_amount_clean"],
            "silver.customers_standardized": ["customer_id", "email_clean"],
        }

        for table, columns in critical_columns.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_records = cursor.fetchone()[0]

                if total_records > 0:
                    for column in columns:
                        cursor.execute(
                            f"""
                            SELECT COUNT(*) FROM {table}
                            WHERE {column} IS NULL
                        """
                        )
                        null_count = cursor.fetchone()[0]
                        null_percentage = (null_count / total_records) * 100

                        assert (
                            null_percentage <= null_threshold
                        ), f"Column {column} in {table} has {null_percentage:.2f}% null values (threshold: {null_threshold}%)"

            except psycopg2.Error:
                pytest.skip(f"Table {table} does not exist")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
