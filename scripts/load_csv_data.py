#!/usr/bin/env python3
"""
Simple CSV Data Loader for MetaLayer ETL Pipeline
Loads CSV files directly into bronze layer tables
"""

import pandas as pd
import psycopg2
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv_to_bronze():
    """Load CSV files into bronze layer tables"""
    
    # Database connection (using Docker network)
    conn_params = {
        'host': 'etl_postgres',
        'database': 'airflow',
        'user': 'postgres',
        'password': 'etl_secure_2024!',
        'port': 5432
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Load CRM customers
        logger.info("Loading CRM customers data...")
        crm_customers = pd.read_csv('/opt/airflow/data/bronze_src/crm/crm_customers.csv')
        
        # Create table if not exists
        create_crm_table = """
        CREATE TABLE IF NOT EXISTS bronze.crm_customers_raw (
            customer_id INTEGER,
            customer_name VARCHAR(255),
            email VARCHAR(255),
            region VARCHAR(100),
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            source_system VARCHAR(50) DEFAULT 'CRM'
        );
        """
        cur.execute(create_crm_table)
        
        # Clear existing data
        cur.execute("TRUNCATE TABLE bronze.crm_customers_raw;")
        
        # Insert data
        for _, row in crm_customers.iterrows():
            insert_sql = """
            INSERT INTO bronze.crm_customers_raw (customer_id, customer_name, email, region)
            VALUES (%s, %s, %s, %s);
            """
            cur.execute(insert_sql, (row['customer_id'], row['customer_name'], row['email'], row['region']))
        
        logger.info(f"Loaded {len(crm_customers)} CRM customer records")
        
        # Load ERP sales data
        logger.info("Loading ERP sales data...")
        erp_sales = pd.read_csv('/opt/airflow/data/bronze_src/erp/erp_sales.csv')
        
        # Create table if not exists
        create_sales_table = """
        CREATE TABLE IF NOT EXISTS bronze.erp_sales_raw (
            sales_id INTEGER,
            product_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            sale_date DATE,
            sale_amount DECIMAL(10,2),
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            source_system VARCHAR(50) DEFAULT 'ERP'
        );
        """
        cur.execute(create_sales_table)
        
        # Clear existing data
        cur.execute("TRUNCATE TABLE bronze.erp_sales_raw;")
        
        # Insert data
        for _, row in erp_sales.iterrows():
            insert_sql = """
            INSERT INTO bronze.erp_sales_raw (sales_id, product_id, customer_id, quantity, sale_date, sale_amount)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_sql, (
                row['sales_id'], row['product_id'], row['customer_id'], 
                row['quantity'], row['sale_date'], row['sale_amount']
            ))
        
        logger.info(f"Loaded {len(erp_sales)} ERP sales records")
        
        # Commit changes
        conn.commit()
        
        # Verify data
        cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
        crm_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
        sales_count = cur.fetchone()[0]
        
        logger.info(f"✅ Data loading completed successfully!")
        logger.info(f"   📊 CRM customers: {crm_count} records")
        logger.info(f"   📊 ERP sales: {sales_count} records")
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {str(e)}")
        raise
    finally:
        try:
            if 'conn' in locals() and conn:
                conn.close()
        except:
            pass

if __name__ == "__main__":
    load_csv_to_bronze()