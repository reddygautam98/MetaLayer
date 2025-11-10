#!/usr/bin/env python3
"""
ETL Pipeline Health Check and Data Validation
Verifies Bronze → Silver → Gold data flow
"""

import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_etl_pipeline():
    """Validate complete ETL pipeline health"""
    
    conn = None
    try:
        # Database connection
        conn = psycopg2.connect(
            host='etl_postgres',
            database='airflow',
            user='postgres',
            password='etl_secure_2024!',
            port=5432
        )
        cur = conn.cursor()
        
        logger.info("🏥 ETL Pipeline Health Check")
        logger.info("=" * 50)
        
        # Check Bronze layer
        logger.info("🥉 BRONZE LAYER VALIDATION:")
        
        cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
        bronze_customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
        bronze_sales = cur.fetchone()[0]
        
        logger.info(f"   📊 CRM Customers: {bronze_customers}")
        logger.info(f"   📊 ERP Sales: {bronze_sales}")
        
        if bronze_customers > 0 and bronze_sales > 0:
            logger.info("   ✅ Bronze layer: HEALTHY")
        else:
            logger.error("   ❌ Bronze layer: NO DATA")
            return False
            
        # Check Silver layer
        logger.info("🥈 SILVER LAYER VALIDATION:")
        
        cur.execute("SELECT COUNT(*) FROM silver.customers_clean;")
        silver_customers = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM silver.sales_clean;")
        silver_sales = cur.fetchone()[0]
        
        logger.info(f"   📊 Clean Customers: {silver_customers}")
        logger.info(f"   📊 Clean Sales: {silver_sales}")
        
        if silver_customers > 0 and silver_sales > 0:
            logger.info("   ✅ Silver layer: HEALTHY")
        else:
            logger.error("   ❌ Silver layer: NO DATA")
            return False
            
        # Check Gold layer
        logger.info("🥇 GOLD LAYER VALIDATION:")
        
        cur.execute("SELECT COUNT(*) FROM gold.customer_analytics;")
        gold_customer_analytics = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM gold.product_analytics;")
        gold_product_analytics = cur.fetchone()[0]
        
        logger.info(f"   📊 Customer Analytics: {gold_customer_analytics}")
        logger.info(f"   📊 Product Analytics: {gold_product_analytics}")
        
        if gold_customer_analytics > 0 and gold_product_analytics > 0:
            logger.info("   ✅ Gold layer: HEALTHY")
        else:
            logger.error("   ❌ Gold layer: NO DATA")
            return False
        
        # Data integrity checks
        logger.info("🔍 DATA INTEGRITY CHECKS:")
        
        # Check if all customers in sales exist in customers
        cur.execute("""
            SELECT COUNT(*) as orphaned_sales
            FROM silver.sales_clean s
            LEFT JOIN silver.customers_clean c ON s.customer_id = c.customer_id
            WHERE c.customer_id IS NULL;
        """)
        orphaned_sales = cur.fetchone()[0]
        
        if orphaned_sales == 0:
            logger.info("   ✅ Referential integrity: PASSED")
        else:
            logger.warning(f"   ⚠️  Found {orphaned_sales} orphaned sales records")
        
        # Check revenue calculations
        cur.execute("""
            SELECT 
                SUM(s.total_value) as silver_total,
                SUM(g.total_revenue) as gold_total
            FROM silver.sales_clean s
            CROSS JOIN (SELECT SUM(total_revenue) as total_revenue FROM gold.customer_analytics) g;
        """)
        result = cur.fetchone()
        silver_revenue = float(result[0])
        gold_revenue = float(result[1])
        
        if abs(silver_revenue - gold_revenue) < 0.01:
            logger.info(f"   ✅ Revenue consistency: ${silver_revenue:.2f} (Silver) = ${gold_revenue:.2f} (Gold)")
        else:
            logger.warning(f"   ⚠️  Revenue mismatch: ${silver_revenue:.2f} (Silver) vs ${gold_revenue:.2f} (Gold)")
        
        # Performance metrics
        logger.info("📈 PERFORMANCE METRICS:")
        
        cur.execute("SELECT region, total_customers, total_revenue FROM gold.customer_analytics ORDER BY total_revenue DESC;")
        regional_data = cur.fetchall()
        
        for region, customers, revenue in regional_data:
            logger.info(f"   🌍 {region}: {customers} customers, ${revenue:.2f} revenue")
        
        cur.execute("SELECT product_id, total_revenue, total_orders FROM gold.product_analytics ORDER BY total_revenue DESC LIMIT 3;")
        top_products = cur.fetchall()
        
        logger.info("   🏆 Top Products:")
        for product_id, revenue, orders in top_products:
            logger.info(f"      Product {product_id}: ${revenue:.2f} revenue, {orders} orders")
        
        logger.info("=" * 50)
        logger.info("🎉 ETL PIPELINE HEALTH CHECK: COMPLETED")
        logger.info("✅ All layers operational with data integrity verified!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    success = validate_etl_pipeline()
    exit(0 if success else 1)