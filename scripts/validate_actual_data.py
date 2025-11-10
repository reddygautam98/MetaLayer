#!/usr/bin/env python3
"""
Actual Data Loading Validation Report
Verifies all actual CSV data is properly loaded and processed
"""

import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_actual_data_loading():
    """Validate that actual CSV data is loaded and processed"""
    
    logger.info("📊 ACTUAL CSV DATA LOADING VALIDATION")
    logger.info("=" * 60)
    
    conn = psycopg2.connect(
        host='etl_postgres',
        database='airflow',
        user='postgres',
        password='etl_secure_2024!',
        port=5432
    )
    cur = conn.cursor()
    
    # Check Bronze layer actual data
    logger.info("🥉 BRONZE LAYER - ACTUAL DATA VALIDATION")
    logger.info("-" * 50)
    
    cur.execute("SELECT COUNT(*) FROM bronze.crm_customers_raw;")
    bronze_customers = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM bronze.erp_sales_raw;")
    bronze_sales = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(customer_id), MAX(customer_id) FROM bronze.crm_customers_raw;")
    crm_range = cur.fetchone()
    
    cur.execute("SELECT MIN(sales_id), MAX(sales_id) FROM bronze.erp_sales_raw;")
    sales_range = cur.fetchone()
    
    logger.info(f"✅ CRM Customers: {bronze_customers:,} records (IDs: {crm_range[0]} to {crm_range[1]})")
    logger.info(f"✅ ERP Sales: {bronze_sales:,} records (IDs: {sales_range[0]} to {sales_range[1]})")
    
    # Regional distribution
    cur.execute("SELECT region, COUNT(*) FROM bronze.crm_customers_raw GROUP BY region ORDER BY COUNT(*) DESC;")
    regions = cur.fetchall()
    
    logger.info("🌍 Regional Distribution:")
    for region, count in regions:
        percentage = (count / bronze_customers) * 100
        logger.info(f"   {region}: {count:,} customers ({percentage:.1f}%)")
    
    # Revenue by year
    cur.execute("SELECT EXTRACT(YEAR FROM sale_date) as year, COUNT(*), SUM(sale_amount) FROM bronze.erp_sales_raw GROUP BY year ORDER BY year;")
    yearly_data = cur.fetchall()
    
    logger.info("📅 Sales by Year:")
    total_revenue = 0
    for year, count, revenue in yearly_data:
        total_revenue += float(revenue)
        logger.info(f"   {int(year)}: {count:,} sales, ${float(revenue):,.2f}")
    
    logger.info(f"💰 Total Revenue: ${total_revenue:,.2f}")
    
    # Check Silver layer processing
    logger.info("\n🥈 SILVER LAYER - DATA PROCESSING VALIDATION")
    logger.info("-" * 50)
    
    cur.execute("SELECT COUNT(*) FROM silver.customers_clean;")
    silver_customers = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM silver.sales_clean;")
    silver_sales = cur.fetchone()[0]
    
    logger.info(f"✅ Processed Customers: {silver_customers:,} records")
    logger.info(f"✅ Processed Sales: {silver_sales:,} records")
    
    # Processing efficiency
    crm_efficiency = (silver_customers / bronze_customers) * 100
    sales_efficiency = (silver_sales / bronze_sales) * 100
    
    logger.info(f"📈 Processing Efficiency:")
    logger.info(f"   CRM: {crm_efficiency:.1f}% records processed")
    logger.info(f"   ERP: {sales_efficiency:.1f}% records processed")
    
    # Email provider analysis
    cur.execute("SELECT email_provider, COUNT(*) FROM silver.customers_clean GROUP BY email_provider ORDER BY COUNT(*) DESC;")
    email_providers = cur.fetchall()
    
    logger.info("📧 Email Provider Distribution:")
    for provider, count in email_providers:
        logger.info(f"   {provider}: {count:,} customers")
    
    # Check Gold layer analytics
    logger.info("\n🥇 GOLD LAYER - ANALYTICS VALIDATION")
    logger.info("-" * 50)
    
    cur.execute("SELECT COUNT(*) FROM gold.customer_analytics;")
    regional_analytics = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM gold.product_analytics;")
    product_analytics = cur.fetchone()[0]
    
    logger.info(f"✅ Regional Analytics: {regional_analytics} regions processed")
    logger.info(f"✅ Product Analytics: {product_analytics} products analyzed")
    
    # Top regions by revenue
    cur.execute("SELECT region, total_customers, total_revenue FROM gold.customer_analytics ORDER BY total_revenue DESC LIMIT 5;")
    top_regions = cur.fetchall()
    
    logger.info("🏆 Top Regions by Revenue:")
    for region, customers, revenue in top_regions:
        logger.info(f"   {region}: {customers:,} customers, ${float(revenue):,.2f}")
    
    # Top products by revenue
    cur.execute("SELECT product_id, total_orders, total_revenue FROM gold.product_analytics ORDER BY total_revenue DESC LIMIT 5;")
    top_products = cur.fetchall()
    
    logger.info("🎯 Top Products by Revenue:")
    for product_id, orders, revenue in top_products:
        logger.info(f"   Product {product_id}: {orders} orders, ${float(revenue):,.2f}")
    
    # Data quality checks
    logger.info("\n🔍 DATA QUALITY VALIDATION")
    logger.info("-" * 50)
    
    # Check for data consistency
    cur.execute("SELECT SUM(total_value) FROM silver.sales_clean WHERE customer_region IS NOT NULL;")
    silver_revenue = cur.fetchone()[0]
    
    cur.execute("SELECT SUM(total_revenue) FROM gold.customer_analytics;")
    gold_revenue = cur.fetchone()[0]
    
    revenue_match = abs(float(silver_revenue) - float(gold_revenue)) < 1.0
    
    logger.info(f"💰 Revenue Consistency Check:")
    logger.info(f"   Silver Total: ${float(silver_revenue):,.2f}")
    logger.info(f"   Gold Total: ${float(gold_revenue):,.2f}")
    logger.info(f"   Match: {'✅ PASSED' if revenue_match else '❌ FAILED'}")
    
    # Check for null values
    cur.execute("SELECT COUNT(*) FROM silver.customers_clean WHERE customer_name_clean IS NULL OR email_clean IS NULL;")
    null_customers = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM silver.sales_clean WHERE total_value IS NULL OR customer_region IS NULL;")
    null_sales = cur.fetchone()[0]
    
    logger.info(f"🔍 Data Integrity:")
    logger.info(f"   Customers with null values: {null_customers}")
    logger.info(f"   Sales with null values: {null_sales}")
    
    # Final assessment
    logger.info("\n🏆 FINAL ASSESSMENT")
    logger.info("-" * 50)
    
    checks = [
        ("Bronze Data Loading", bronze_customers >= 10000 and bronze_sales >= 10000),
        ("Silver Processing", silver_customers >= 9000 and silver_sales >= 9000),
        ("Gold Analytics", regional_analytics >= 5 and product_analytics >= 50),
        ("Revenue Consistency", revenue_match),
        ("Data Quality", null_customers < 100 and null_sales < 100)
    ]
    
    passed_checks = sum(1 for _, passed in checks if passed)
    total_checks = len(checks)
    
    logger.info("📋 Validation Results:")
    for check_name, passed in checks:
        icon = "✅" if passed else "❌"
        logger.info(f"   {icon} {check_name}: {'PASSED' if passed else 'FAILED'}")
    
    score = (passed_checks / total_checks) * 100
    logger.info(f"\n🎯 Overall Score: {score:.1f}% ({passed_checks}/{total_checks})")
    
    if score >= 90:
        status = "🟢 EXCELLENT - All actual data loaded and processed successfully"
    elif score >= 75:
        status = "🟡 GOOD - Most data loaded with minor issues"
    else:
        status = "🔴 ISSUES - Significant problems detected"
    
    logger.info(f"🏅 Status: {status}")
    
    logger.info("\n" + "=" * 60)
    logger.info("🎉 ACTUAL CSV DATA VALIDATION COMPLETE!")
    logger.info(f"📊 Total Records: {bronze_customers + bronze_sales:,}")
    logger.info(f"💼 Business Ready: {'YES' if score >= 90 else 'NEEDS ATTENTION'}")
    
    conn.close()
    return score >= 80

if __name__ == "__main__":
    validate_actual_data_loading()