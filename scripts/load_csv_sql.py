#!/usr/bin/env python3
"""
Simple SQL-based CSV Data Loader for MetaLayer ETL Pipeline
Uses SQL COPY command for efficient loading
"""

import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv_with_sql():
    """Load CSV files using SQL COPY command"""
    
    conn = None
    try:
        # Database connection (using Docker network)
        conn = psycopg2.connect(
            host='etl_postgres',
            database='airflow',
            user='postgres',
            password='etl_secure_2024!',
            port=5432
        )
        cur = conn.cursor()
        
        logger.info("🔄 Creating bronze layer tables...")
        
        # Create CRM customers table
        create_crm_table = """
        DROP TABLE IF EXISTS bronze.crm_customers_raw CASCADE;
        CREATE TABLE bronze.crm_customers_raw (
            customer_id INTEGER,
            customer_name VARCHAR(255),
            email VARCHAR(255),
            region VARCHAR(100),
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            source_system VARCHAR(50) DEFAULT 'CRM'
        );
        """
        cur.execute(create_crm_table)
        logger.info("✅ CRM customers table created")
        
        # Create ERP sales table
        create_sales_table = """
        DROP TABLE IF EXISTS bronze.erp_sales_raw CASCADE;
        CREATE TABLE bronze.erp_sales_raw (
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
        logger.info("✅ ERP sales table created")
        
        # Load CRM data using COPY command
        logger.info("📊 Loading CRM customers data...")
        copy_crm_sql = """
        COPY bronze.crm_customers_raw (customer_id, customer_name, email, region)
        FROM '/opt/airflow/data/bronze_src/crm/crm_customers.csv'
        WITH (FORMAT csv, HEADER true);
        """
        cur.execute(copy_crm_sql)
        
        # Get count
        cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
        crm_count = cur.fetchone()[0]
        logger.info(f"✅ Loaded {crm_count} CRM customer records")
        
        # Load ERP data using COPY command
        logger.info("📊 Loading ERP sales data...")
        copy_erp_sql = """
        COPY bronze.erp_sales_raw (sales_id, product_id, customer_id, quantity, sale_date, sale_amount)
        FROM '/opt/airflow/data/bronze_src/erp/erp_sales.csv'
        WITH (FORMAT csv, HEADER true);
        """
        cur.execute(copy_erp_sql)
        
        # Get count
        cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
        sales_count = cur.fetchone()[0]
        logger.info(f"✅ Loaded {sales_count} ERP sales records")
        
        # Commit changes
        conn.commit()
        
        logger.info("🎉 Data loading completed successfully!")
        logger.info(f"   📊 Total CRM customers: {crm_count:,}")
        logger.info(f"   📊 Total ERP sales: {sales_count:,}")
        
        # Show sample data
        logger.info("📋 Sample CRM data:")
        cur.execute("SELECT * FROM bronze.crm_customers_raw LIMIT 3;")
        for row in cur.fetchall():
            logger.info(f"   {row}")
            
        logger.info("📋 Sample ERP data:")
        cur.execute("SELECT * FROM bronze.erp_sales_raw LIMIT 3;")
        for row in cur.fetchall():
            logger.info(f"   {row}")
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_csv_with_sql()