#!/usr/bin/env python3
"""
Simple Sample Data Loader for MetaLayer ETL Pipeline
Creates sample data for testing the complete pipeline
"""

import psycopg2
import logging
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_sample_data():
    """Load sample data into bronze layer tables"""
    
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
        
        # Sample CRM customers data
        crm_customers = [
            (1, 'John Doe', 'john.doe@email.com', 'North'),
            (2, 'Jane Smith', 'jane.smith@email.com', 'South'),
            (3, 'Bob Johnson', 'bob.johnson@email.com', 'East'),
            (4, 'Alice Brown', 'alice.brown@email.com', 'West'),
            (5, 'Charlie Wilson', 'charlie.wilson@email.com', 'North'),
            (6, 'Diana Davis', 'diana.davis@email.com', 'South'),
            (7, 'Frank Miller', 'frank.miller@email.com', 'East'),
            (8, 'Grace Lee', 'grace.lee@email.com', 'West'),
            (9, 'Henry Garcia', 'henry.garcia@email.com', 'North'),
            (10, 'Ivy Martinez', 'ivy.martinez@email.com', 'South')
        ]
        
        # Insert CRM data
        logger.info("📊 Loading CRM customers data...")
        for customer in crm_customers:
            cur.execute("""
                INSERT INTO bronze.crm_customers_raw (customer_id, customer_name, email, region)
                VALUES (%s, %s, %s, %s)
            """, customer)
        
        # Sample ERP sales data
        erp_sales = [
            (1, 101, 1, 2, '2024-01-15', 299.99),
            (2, 102, 2, 1, '2024-01-16', 149.50),
            (3, 103, 3, 3, '2024-01-17', 89.99),
            (4, 101, 4, 1, '2024-01-18', 299.99),
            (5, 104, 5, 2, '2024-01-19', 199.99),
            (6, 102, 6, 4, '2024-01-20', 598.00),
            (7, 105, 7, 1, '2024-01-21', 79.99),
            (8, 103, 8, 2, '2024-01-22', 179.98),
            (9, 106, 9, 1, '2024-01-23', 449.99),
            (10, 104, 10, 3, '2024-01-24', 599.97),
            (11, 107, 1, 1, '2024-02-01', 99.99),
            (12, 108, 2, 2, '2024-02-02', 249.98),
            (13, 109, 3, 1, '2024-02-03', 399.99),
            (14, 110, 4, 1, '2024-02-04', 199.99),
            (15, 101, 5, 2, '2024-02-05', 599.98)
        ]
        
        # Insert ERP data
        logger.info("📊 Loading ERP sales data...")
        for sale in erp_sales:
            cur.execute("""
                INSERT INTO bronze.erp_sales_raw (sales_id, product_id, customer_id, quantity, sale_date, sale_amount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, sale)
        
        # Commit changes
        conn.commit()
        
        # Get counts
        cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
        crm_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
        sales_count = cur.fetchone()[0]
        
        logger.info("🎉 Sample data loading completed successfully!")
        logger.info(f"   📊 Total CRM customers: {crm_count}")
        logger.info(f"   📊 Total ERP sales: {sales_count}")
        
        # Show sample data
        logger.info("📋 Sample CRM data:")
        cur.execute("SELECT customer_id, customer_name, region FROM bronze.crm_customers_raw LIMIT 5;")
        for row in cur.fetchall():
            logger.info(f"   Customer {row[0]}: {row[1]} ({row[2]})")
            
        logger.info("📋 Sample ERP data:")
        cur.execute("SELECT sales_id, customer_id, product_id, sale_amount FROM bronze.erp_sales_raw LIMIT 5;")
        for row in cur.fetchall():
            logger.info(f"   Sale {row[0]}: Customer {row[1]} bought Product {row[2]} for ${row[3]}")
        
        # Now create Silver layer transformations
        logger.info("🔄 Creating Silver layer tables...")
        
        create_silver_customers = """
        DROP TABLE IF EXISTS silver.customers_clean CASCADE;
        CREATE TABLE silver.customers_clean AS
        SELECT 
            customer_id,
            TRIM(UPPER(customer_name)) as customer_name_clean,
            LOWER(TRIM(email)) as email_clean,
            UPPER(region) as region_standardized,
            CASE 
                WHEN email LIKE '%@gmail.com' THEN 'Gmail'
                WHEN email LIKE '%@yahoo.com' THEN 'Yahoo'
                WHEN email LIKE '%@email.com' THEN 'Generic'
                ELSE 'Other'
            END as email_provider,
            NOW() as processed_timestamp
        FROM bronze.crm_customers_raw
        WHERE customer_name IS NOT NULL 
        AND email IS NOT NULL
        AND region IS NOT NULL;
        """
        cur.execute(create_silver_customers)
        
        create_silver_sales = """
        DROP TABLE IF EXISTS silver.sales_clean CASCADE;
        CREATE TABLE silver.sales_clean AS
        SELECT 
            s.sales_id,
            s.customer_id,
            s.product_id,
            s.quantity,
            s.sale_date,
            s.sale_amount,
            s.quantity * s.sale_amount as total_value,
            EXTRACT(MONTH FROM s.sale_date) as sale_month,
            EXTRACT(YEAR FROM s.sale_date) as sale_year,
            c.region_standardized as customer_region,
            NOW() as processed_timestamp
        FROM bronze.erp_sales_raw s
        JOIN silver.customers_clean c ON s.customer_id = c.customer_id
        WHERE s.quantity > 0 
        AND s.sale_amount > 0;
        """
        cur.execute(create_silver_sales)
        
        # Now create Gold layer analytics
        logger.info("🔄 Creating Gold layer analytics...")
        
        create_gold_customer_analytics = """
        DROP TABLE IF EXISTS gold.customer_analytics CASCADE;
        CREATE TABLE gold.customer_analytics AS
        SELECT 
            c.region_standardized as region,
            COUNT(DISTINCT c.customer_id) as total_customers,
            COUNT(s.sales_id) as total_sales,
            SUM(s.total_value) as total_revenue,
            AVG(s.total_value) as avg_sale_value,
            MAX(s.sale_date) as last_sale_date,
            NOW() as analytics_timestamp
        FROM silver.customers_clean c
        LEFT JOIN silver.sales_clean s ON c.customer_id = s.customer_id
        GROUP BY c.region_standardized;
        """
        cur.execute(create_gold_customer_analytics)
        
        create_gold_product_analytics = """
        DROP TABLE IF EXISTS gold.product_analytics CASCADE;
        CREATE TABLE gold.product_analytics AS
        SELECT 
            product_id,
            COUNT(sales_id) as total_orders,
            SUM(quantity) as total_quantity_sold,
            SUM(total_value) as total_revenue,
            AVG(total_value) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers,
            NOW() as analytics_timestamp
        FROM silver.sales_clean
        GROUP BY product_id
        ORDER BY total_revenue DESC;
        """
        cur.execute(create_gold_product_analytics)
        
        conn.commit()
        
        # Show final results
        logger.info("📊 BRONZE → SILVER → GOLD Pipeline Complete!")
        
        cur.execute("SELECT COUNT(*) FROM silver.customers_clean;")
        silver_customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM silver.sales_clean;")
        silver_sales = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM gold.customer_analytics;")
        gold_customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM gold.product_analytics;")
        gold_products = cur.fetchone()[0]
        
        logger.info(f"   🥉 Bronze: {crm_count} customers, {sales_count} sales")
        logger.info(f"   🥈 Silver: {silver_customers} customers, {silver_sales} sales")
        logger.info(f"   🥇 Gold: {gold_customers} regional analytics, {gold_products} product analytics")
        
        # Show gold analytics
        logger.info("🏆 Gold Layer - Regional Analytics:")
        cur.execute("SELECT region, total_customers, total_revenue FROM gold.customer_analytics ORDER BY total_revenue DESC;")
        for row in cur.fetchall():
            logger.info(f"   {row[0]}: {row[1]} customers, ${row[2]:.2f} revenue")
            
        logger.info("🏆 Gold Layer - Top Products:")
        cur.execute("SELECT product_id, total_revenue, total_orders FROM gold.product_analytics LIMIT 5;")
        for row in cur.fetchall():
            logger.info(f"   Product {row[0]}: ${row[1]:.2f} revenue, {row[2]} orders")
        
    except Exception as e:
        logger.error(f"❌ Error loading data: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_sample_data()