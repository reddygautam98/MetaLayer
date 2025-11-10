#!/usr/bin/env python3
"""
Real CSV Data Loader for MetaLayer ETL Pipeline
Loads the actual large CSV files (1M+ records) into Bronze layer
"""

import psycopg2
import logging
import time
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@contextmanager
def get_db_connection():
    """Database connection context manager"""
    conn = None
    try:
        conn = psycopg2.connect(
            host='etl_postgres',
            database='airflow',
            user='postgres',
            password='etl_secure_2024!',
            port=5432
        )
        yield conn
    finally:
        if conn:
            conn.close()

def load_actual_csv_data():
    """Load the actual large CSV files into Bronze layer"""
    
    logger.info("🚀 LOADING ACTUAL CSV DATA INTO BRONZE LAYER")
    logger.info("=" * 60)
    
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # 1. Drop existing tables to start fresh
        logger.info("🗑️ Clearing existing Bronze layer tables...")
        
        cur.execute("DROP TABLE IF EXISTS bronze.crm_customers_raw CASCADE;")
        cur.execute("DROP TABLE IF EXISTS bronze.erp_sales_raw CASCADE;")
        
        # 2. Create Bronze layer tables optimized for large data
        logger.info("🏗️ Creating optimized Bronze layer tables...")
        
        create_crm_table = """
        CREATE TABLE bronze.crm_customers_raw (
            customer_id INTEGER PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            region VARCHAR(100),
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            source_system VARCHAR(50) DEFAULT 'CRM'
        );
        """
        cur.execute(create_crm_table)
        
        create_sales_table = """
        CREATE TABLE bronze.erp_sales_raw (
            sales_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            customer_id INTEGER,
            quantity INTEGER CHECK (quantity > 0),
            sale_date DATE,
            sale_amount DECIMAL(10,2) CHECK (sale_amount >= 0),
            ingestion_timestamp TIMESTAMP DEFAULT NOW(),
            source_system VARCHAR(50) DEFAULT 'ERP'
        );
        """
        cur.execute(create_sales_table)
        
        # Create indexes for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_crm_region ON bronze.crm_customers_raw(region);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_customer ON bronze.erp_sales_raw(customer_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_date ON bronze.erp_sales_raw(sale_date);")
        
        logger.info("✅ Bronze layer tables created with indexes")
        
        # 3. Load CRM Customers data using COPY
        logger.info("📊 Loading CRM Customers data...")
        start_time = time.time()
        
        try:
            copy_crm_sql = """
            COPY bronze.crm_customers_raw (customer_id, customer_name, email, region)
            FROM '/opt/airflow/data/bronze_src/crm/crm_customers.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
            """
            cur.execute(copy_crm_sql)
            
            cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
            crm_count = cur.fetchone()[0]
            crm_time = time.time() - start_time
            
            logger.info(f"✅ CRM Customers loaded: {crm_count:,} records in {crm_time:.2f} seconds")
            
        except Exception as e:
            logger.warning(f"⚠️ Direct COPY failed: {str(e)}")
            logger.info("🔄 Falling back to batch insert method...")
            
            # Fallback: Read and insert in batches
            cur.execute("TRUNCATE TABLE bronze.crm_customers_raw;")
            
            # Use psql command for large file loading
            import subprocess
            result = subprocess.run([
                'docker', 'exec', 'etl_postgres', 'psql', 
                '-U', 'postgres', '-d', 'airflow', 
                '-c', copy_crm_sql
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
                crm_count = cur.fetchone()[0]
                logger.info(f"✅ CRM Customers loaded via psql: {crm_count:,} records")
            else:
                logger.error(f"❌ Failed to load CRM data: {result.stderr}")
                raise Exception("CRM data loading failed")
        
        # 4. Load ERP Sales data using COPY
        logger.info("📊 Loading ERP Sales data...")
        start_time = time.time()
        
        try:
            copy_erp_sql = """
            COPY bronze.erp_sales_raw (sales_id, product_id, customer_id, quantity, sale_date, sale_amount)
            FROM '/opt/airflow/data/bronze_src/erp/erp_sales.csv'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');
            """
            cur.execute(copy_erp_sql)
            
            cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
            sales_count = cur.fetchone()[0]
            sales_time = time.time() - start_time
            
            logger.info(f"✅ ERP Sales loaded: {sales_count:,} records in {sales_time:.2f} seconds")
            
        except Exception as e:
            logger.warning(f"⚠️ Direct COPY failed: {str(e)}")
            logger.info("🔄 Falling back to psql method...")
            
            # Fallback: Use psql command
            cur.execute("TRUNCATE TABLE bronze.erp_sales_raw;")
            
            import subprocess
            result = subprocess.run([
                'docker', 'exec', 'etl_postgres', 'psql', 
                '-U', 'postgres', '-d', 'airflow', 
                '-c', copy_erp_sql
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
                sales_count = cur.fetchone()[0]
                logger.info(f"✅ ERP Sales loaded via psql: {sales_count:,} records")
            else:
                logger.error(f"❌ Failed to load ERP data: {result.stderr}")
                raise Exception("ERP data loading failed")
        
        # 5. Commit all changes
        conn.commit()
        
        # 6. Generate data quality statistics
        logger.info("📈 Generating data quality statistics...")
        
        # CRM statistics
        cur.execute("SELECT region, COUNT(*) FROM bronze.crm_customers_raw GROUP BY region ORDER BY COUNT(*) DESC;")
        crm_regions = cur.fetchall()
        
        logger.info("🌍 CRM Customer Distribution by Region:")
        for region, count in crm_regions:
            logger.info(f"   {region}: {count:,} customers")
        
        # ERP statistics
        cur.execute("SELECT EXTRACT(YEAR FROM sale_date) as year, COUNT(*), SUM(sale_amount) FROM bronze.erp_sales_raw GROUP BY year ORDER BY year;")
        sales_yearly = cur.fetchall()
        
        logger.info("📅 ERP Sales Distribution by Year:")
        for year, count, total_amount in sales_yearly:
            logger.info(f"   {int(year)}: {count:,} sales, ${float(total_amount):,.2f} revenue")
        
        # Data quality checks
        cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw WHERE email IS NULL OR email = '';")
        null_emails = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw WHERE sale_amount <= 0 OR quantity <= 0;")
        invalid_sales = cur.fetchone()[0]
        
        logger.info("🔍 Data Quality Checks:")
        logger.info(f"   CRM records with missing emails: {null_emails:,}")
        logger.info(f"   ERP records with invalid amounts/quantities: {invalid_sales:,}")
        
        # Sample data
        logger.info("📋 Sample Bronze Layer Data:")
        cur.execute("SELECT customer_id, customer_name, region FROM bronze.crm_customers_raw LIMIT 5;")
        for row in cur.fetchall():
            logger.info(f"   Customer {row[0]}: {row[1]} ({row[2]})")
        
        cur.execute("SELECT sales_id, customer_id, product_id, sale_amount FROM bronze.erp_sales_raw ORDER BY sale_amount DESC LIMIT 5;")
        logger.info("💰 Top ERP Sales:")
        for row in cur.fetchall():
            logger.info(f"   Sale {row[0]}: Customer {row[1]}, Product {row[2]}, ${float(row[3]):.2f}")
        
        logger.info("=" * 60)
        logger.info("🎉 ACTUAL CSV DATA LOADING COMPLETED!")
        logger.info(f"📊 Total Records Loaded: {crm_count + sales_count:,}")
        logger.info("✅ Bronze layer ready for Silver layer processing")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = load_actual_csv_data()
    exit(0 if success else 1)