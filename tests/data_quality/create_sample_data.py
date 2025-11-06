"""
Sample Data Creation Script for MetaLayer Testing
Creates sample data for testing data quality and pipeline validation
"""

import psycopg2
import pandas as pd
import os
from datetime import datetime, timedelta
import random


def create_sample_data():
    """Create sample data for testing"""

    # Database connection
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://test_user:test_password@localhost:5432/test_airflow",
    )

    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        print("Creating sample test data...")

        # Create schemas if they don't exist
        schemas = ["bronze", "silver", "gold"]
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Create bronze layer tables
        create_bronze_tables(cursor)

        # Insert sample data
        insert_sample_bronze_data(cursor)

        # Create silver layer tables
        create_silver_tables(cursor)

        # Insert sample silver data
        insert_sample_silver_data(cursor)

        # Create gold layer tables
        create_gold_tables(cursor)

        # Insert sample gold data
        insert_sample_gold_data(cursor)

        conn.commit()
        print("✅ Sample data created successfully!")

    except Exception as e:
        print(f"❌ Error creating sample data: {e}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def create_bronze_tables(cursor):
    """Create bronze layer tables"""

    # ERP Sales Raw Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.erp_sales_raw (
            sales_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            product_code VARCHAR(50),
            quantity INTEGER,
            sale_amount DECIMAL(10,2),
            sale_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_date_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # CRM Customers Raw Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.crm_customers_raw (
            customer_id VARCHAR(50) PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(200),
            phone VARCHAR(50),
            city VARCHAR(100),
            state VARCHAR(50),
            zip VARCHAR(20),
            dob DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_date_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def create_silver_tables(cursor):
    """Create silver layer tables"""

    # Sales Cleaned Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.sales_cleaned (
            sales_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            product_code_clean VARCHAR(50),
            quantity_clean INTEGER,
            sale_amount_clean DECIMAL(10,2),
            sale_date_clean DATE,
            processed_date_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Customers Standardized Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.customers_standardized (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_name_clean VARCHAR(200),
            email_clean VARCHAR(200),
            phone_clean VARCHAR(50),
            city_clean VARCHAR(100),
            state_clean VARCHAR(50),
            zip_clean VARCHAR(20),
            dob_clean DATE,
            processed_date_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def create_gold_tables(cursor):
    """Create gold layer tables"""

    # Dimension Customer Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id VARCHAR(50) UNIQUE,
            customer_name VARCHAR(200),
            email_clean VARCHAR(200),
            city VARCHAR(100),
            state VARCHAR(50),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Fact Sales Table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            sales_key SERIAL PRIMARY KEY,
            sales_id VARCHAR(50) UNIQUE,
            customer_key INTEGER REFERENCES gold.dim_customer(customer_key),
            product_code VARCHAR(50),
            quantity INTEGER,
            total_amount DECIMAL(10,2),
            sale_date DATE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def insert_sample_bronze_data(cursor):
    """Insert sample data into bronze tables"""

    # Sample ERP Sales Data
    sales_data = [
        ("S001", "C001", "P001", 2, 150.00, "2024-01-01"),
        ("S002", "C002", "P002", 1, 75.50, "2024-01-02"),
        ("S003", "C001", "P003", 3, 225.75, "2024-01-03"),
        ("S004", "C003", "P001", 1, 75.00, "2024-01-04"),
        ("S005", "C002", "P004", 5, 375.25, "2024-01-05"),
        ("S006", "C004", "P002", 2, 151.00, "2024-01-06"),
        ("S007", "C005", "P003", 4, 300.00, "2024-01-07"),
        ("S008", "C003", "P005", 1, 89.99, "2024-01-08"),
        ("S009", "C001", "P004", 2, 150.50, "2024-01-09"),
        ("S010", "C004", "P001", 3, 225.00, "2024-01-10"),
    ]

    cursor.executemany(
        """
        INSERT INTO bronze.erp_sales_raw (sales_id, customer_id, product_code, quantity, sale_amount, sale_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sales_id) DO NOTHING
    """,
        sales_data,
    )

    # Sample CRM Customers Data
    customer_data = [
        (
            "C001",
            "John",
            "Doe",
            "john.doe@email.com",
            "555-1234",
            "New York",
            "NY",
            "10001",
            "1985-01-15",
        ),
        (
            "C002",
            "Jane",
            "Smith",
            "jane.smith@email.com",
            "555-5678",
            "Los Angeles",
            "CA",
            "90001",
            "1990-05-20",
        ),
        (
            "C003",
            "Bob",
            "Johnson",
            "bob.johnson@email.com",
            "555-9012",
            "Chicago",
            "IL",
            "60601",
            "1988-12-10",
        ),
        (
            "C004",
            "Alice",
            "Brown",
            "alice.brown@email.com",
            "555-3456",
            "Houston",
            "TX",
            "77001",
            "1992-08-25",
        ),
        (
            "C005",
            "Charlie",
            "Wilson",
            "charlie.wilson@email.com",
            "555-7890",
            "Phoenix",
            "AZ",
            "85001",
            "1987-03-18",
        ),
    ]

    cursor.executemany(
        """
        INSERT INTO bronze.crm_customers_raw (customer_id, first_name, last_name, email, phone, city, state, zip, dob)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """,
        customer_data,
    )


def insert_sample_silver_data(cursor):
    """Insert sample data into silver tables (cleaned and standardized)"""

    # Clean sales data
    cursor.execute(
        """
        INSERT INTO silver.sales_cleaned (
            sales_id, customer_id, product_code_clean, quantity_clean, 
            sale_amount_clean, sale_date_clean
        )
        SELECT 
            sales_id,
            customer_id,
            UPPER(TRIM(product_code)) as product_code_clean,
            quantity,
            sale_amount,
            sale_date
        FROM bronze.erp_sales_raw
        WHERE sale_amount > 0 AND quantity > 0
        ON CONFLICT (sales_id) DO NOTHING
    """
    )

    # Clean customer data
    cursor.execute(
        """
        INSERT INTO silver.customers_standardized (
            customer_id, customer_name_clean, email_clean, phone_clean,
            city_clean, state_clean, zip_clean, dob_clean
        )
        SELECT 
            customer_id,
            TRIM(INITCAP(first_name)) || ' ' || TRIM(INITCAP(last_name)) as customer_name_clean,
            LOWER(TRIM(email)) as email_clean,
            phone,
            TRIM(INITCAP(city)) as city_clean,
            UPPER(TRIM(state)) as state_clean,
            zip,
            dob
        FROM bronze.crm_customers_raw
        WHERE email IS NOT NULL AND email LIKE '%@%'
        ON CONFLICT (customer_id) DO NOTHING
    """
    )


def insert_sample_gold_data(cursor):
    """Insert sample data into gold tables (dimensional model)"""

    # Insert dimension customer data
    cursor.execute(
        """
        INSERT INTO gold.dim_customer (customer_id, customer_name, email_clean, city, state)
        SELECT 
            customer_id,
            customer_name_clean,
            email_clean,
            city_clean,
            state_clean
        FROM silver.customers_standardized
        ON CONFLICT (customer_id) DO NOTHING
    """
    )

    # Insert fact sales data
    cursor.execute(
        """
        INSERT INTO gold.fact_sales (
            sales_id, customer_key, product_code, quantity, total_amount, sale_date
        )
        SELECT 
            s.sales_id,
            d.customer_key,
            s.product_code_clean,
            s.quantity_clean,
            s.sale_amount_clean,
            s.sale_date_clean
        FROM silver.sales_cleaned s
        JOIN gold.dim_customer d ON s.customer_id = d.customer_id
        ON CONFLICT (sales_id) DO NOTHING
    """
    )


if __name__ == "__main__":
    create_sample_data()
