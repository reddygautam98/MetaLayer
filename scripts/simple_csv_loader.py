#!/usr/bin/env python3
"""
Load Actual CSV Data - Simplified Version
"""

import psycopg2
import pandas as pd
import logging
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_csv_data():
    conn = psycopg2.connect(
        host='etl_postgres',
        database='airflow',
        user='postgres',
        password='etl_secure_2024!',
        port=5432
    )
    cur = conn.cursor()
    
    logger.info("Loading CRM customers...")
    
    # Read first 10000 records to test
    crm_df = pd.read_csv('/opt/airflow/data/bronze_src/crm/crm_customers.csv', nrows=10000)
    logger.info(f"Read {len(crm_df)} CRM records")
    
    # Insert in batches
    for i, row in crm_df.iterrows():
        cur.execute("""
            INSERT INTO bronze.crm_customers_raw (customer_id, customer_name, email, region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """, (row['customer_id'], row['customer_name'], row['email'], row['region']))
        
        if i % 1000 == 0:
            logger.info(f"Inserted {i+1} CRM records...")
            conn.commit()
    
    conn.commit()
    
    logger.info("Loading ERP sales...")
    
    # Read first 10000 records to test
    erp_df = pd.read_csv('/opt/airflow/data/bronze_src/erp/erp_sales.csv', nrows=10000)
    logger.info(f"Read {len(erp_df)} ERP records")
    
    # Insert in batches
    for i, row in erp_df.iterrows():
        cur.execute("""
            INSERT INTO bronze.erp_sales_raw (sales_id, product_id, customer_id, quantity, sale_date, sale_amount)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sales_id) DO NOTHING;
        """, (row['sales_id'], row['product_id'], row['customer_id'], row['quantity'], row['sale_date'], row['sale_amount']))
        
        if i % 1000 == 0:
            logger.info(f"Inserted {i+1} ERP records...")
            conn.commit()
    
    conn.commit()
    
    # Check final counts
    cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
    crm_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
    erp_count = cur.fetchone()[0]
    
    logger.info(f"Final counts: CRM={crm_count}, ERP={erp_count}")
    
    conn.close()
    return True

if __name__ == "__main__":
    load_csv_data()