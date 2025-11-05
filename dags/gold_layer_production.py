import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

def validate_gold_layer():
    """Validate Gold layer data quality and business metrics"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Validate dimensional model integrity
        validation_queries = {
            'dim_customer_integrity': '''
                SELECT COUNT(*) FROM gold.dim_customer 
                WHERE customer_id IS NULL OR customer_name IS NULL
            ''',
            'fact_sales_integrity': '''
                SELECT COUNT(*) FROM gold.fact_sales 
                WHERE customer_key IS NULL OR order_date IS NULL OR total_amount <= 0
            ''',
            'foreign_key_integrity': '''
                SELECT COUNT(*) FROM gold.fact_sales f
                LEFT JOIN gold.dim_customer d ON f.customer_key = d.customer_key
                WHERE d.customer_key IS NULL
            ''',
            'monthly_summary_completeness': '''
                SELECT COUNT(*) FROM gold.monthly_sales_summary
                WHERE total_transactions IS NULL OR total_revenue IS NULL
            ''',
            'regional_performance_completeness': '''
                SELECT COUNT(*) FROM gold.regional_performance
                WHERE region_clean IS NULL OR total_revenue IS NULL
            '''
        }
        
        validation_results = {}
        integrity_issues = []
        
        for check_name, query in validation_queries.items():
            issue_count = hook.get_first(query)[0]
            validation_results[check_name] = issue_count
            
            if issue_count > 0:
                integrity_issues.append(f"{check_name}: {issue_count} issues")
        
        # Business metrics validation
        business_metrics = hook.get_records('''
            SELECT 
                'total_revenue' as metric,
                SUM(total_revenue) as value
            FROM gold.monthly_sales_summary
            UNION ALL
            SELECT 
                'total_customers' as metric,
                COUNT(*) as value
            FROM gold.dim_customer
            UNION ALL
            SELECT 
                'avg_monthly_transactions' as metric,
                AVG(total_transactions) as value
            FROM gold.monthly_sales_summary
        ''')
        
        business_results = {metric: float(value) for metric, value in business_metrics}
        
        # Validate business thresholds
        min_revenue = 1000000  # Minimum expected revenue
        min_customers = 100000  # Minimum expected customers
        
        if business_results.get('total_revenue', 0) < min_revenue:
            integrity_issues.append(f"Total revenue too low: {business_results.get('total_revenue', 0)} < {min_revenue}")
        
        if business_results.get('total_customers', 0) < min_customers:
            integrity_issues.append(f"Customer count too low: {business_results.get('total_customers', 0)} < {min_customers}")
        
        # Final validation result
        validation_summary = {
            'validation_timestamp': datetime.utcnow().isoformat(),
            'integrity_checks': validation_results,
            'business_metrics': business_results,
            'integrity_issues': integrity_issues,
            'validation_status': 'PASSED' if not integrity_issues else 'FAILED'
        }
        
        if integrity_issues:
            logger.error(f"Gold layer validation failed: {integrity_issues}")
            raise AirflowException(f"Gold layer validation failed: {integrity_issues}")
        
        logger.info(f"Gold layer validation passed: {validation_summary}")
        return validation_summary
        
    except Exception as e:
        logger.error(f"Gold layer validation error: {str(e)}")
        raise AirflowException(f"Gold layer validation error: {str(e)}")

def generate_business_intelligence_report():
    """Generate BI-ready business intelligence report"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Generate comprehensive business report
        report_queries = {
            'revenue_summary': '''
                SELECT 
                    SUM(total_revenue) as total_revenue,
                    AVG(total_revenue) as avg_monthly_revenue,
                    COUNT(*) as reporting_months,
                    MIN(period_start) as earliest_date,
                    MAX(period_end) as latest_date
                FROM gold.monthly_sales_summary
            ''',
            'top_regions': '''
                SELECT 
                    region_clean,
                    total_revenue,
                    total_customers,
                    ROUND(revenue_per_customer, 2) as revenue_per_customer
                FROM gold.regional_performance
                ORDER BY total_revenue DESC
                LIMIT 5
            ''',
            'top_products': '''
                SELECT 
                    product_id,
                    total_revenue,
                    total_sales,
                    ROUND(avg_sale_price, 2) as avg_sale_price,
                    revenue_rank
                FROM gold.product_performance
                ORDER BY total_revenue DESC
                LIMIT 10
            ''',
            'customer_segments': '''
                SELECT 
                    CASE 
                        WHEN lifetime_value >= 10000 THEN 'Premium'
                        WHEN lifetime_value >= 5000 THEN 'Gold'
                        WHEN lifetime_value >= 1000 THEN 'Silver'
                        ELSE 'Bronze'
                    END as customer_tier,
                    COUNT(*) as customer_count,
                    SUM(lifetime_value) as tier_revenue,
                    ROUND(AVG(lifetime_value), 2) as avg_lifetime_value
                FROM silver.v_customer_summary
                GROUP BY 1
                ORDER BY avg_lifetime_value DESC
            '''
        }
        
        bi_report = {
            'report_generated': datetime.utcnow().isoformat(),
            'report_type': 'executive_summary'
        }
        
        for section, query in report_queries.items():
            results = hook.get_records(query)
            bi_report[section] = results
        
        # Log key insights
        revenue_summary = bi_report['revenue_summary'][0] if bi_report['revenue_summary'] else None
        if revenue_summary:
            logger.info(f"Business Intelligence Summary:")
            logger.info(f"  Total Revenue: ${revenue_summary[0]:,.2f}")
            logger.info(f"  Avg Monthly Revenue: ${revenue_summary[1]:,.2f}")
            logger.info(f"  Reporting Period: {revenue_summary[3]} to {revenue_summary[4]}")
        
        return bi_report
        
    except Exception as e:
        logger.error(f"BI report generation failed: {str(e)}")
        raise AirflowException(f"BI report generation failed: {str(e)}")

with DAG(
    dag_id='gold_layer_production_analytics',
    default_args=default_args,
    description='Production Gold Layer - Build Analytics & BI Ready Dimensional Model',
    schedule=None,  # Triggered after silver layer completion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'production', 'analytics', 'bi', 'dimensional-model'],
    doc_md="""
    # Gold Layer Production Analytics
    
    This DAG creates production-ready analytics tables and dimensional models:
    - Customer dimension with business attributes
    - Sales fact table with metrics
    - Pre-aggregated analytics tables
    - Business intelligence views
    - Data quality validation
    
    ## Output Tables:
    - gold.dim_customer (Customer dimension)
    - gold.fact_sales (Sales facts)  
    - gold.monthly_sales_summary (Time-series aggregations)
    - gold.regional_performance (Geographic analysis)
    - gold.product_performance (Product analytics)
    
    ## Business Intelligence:
    - Executive dashboards
    - Regional performance analysis
    - Customer segmentation
    - Product profitability
    """
):

    # Wait for silver layer to complete
    wait_for_silver = ExternalTaskSensor(
        task_id='wait_for_silver_layer',
        external_dag_id='silver_layer_production_transform',
        external_task_id='log_transformation_metrics',
        timeout=3600,
        poke_interval=60,
        mode='poke',
        doc_md="Wait for silver layer transformations to complete"
    )

    # Create Gold layer dimensional model
    create_dimensional_model = SQLExecuteQueryOperator(
        task_id='create_dimensional_model',
        conn_id='postgres_default',
        sql='''
        -- Customer Dimension (Type 1 SCD)
        DROP TABLE IF EXISTS gold.dim_customer CASCADE;
        CREATE TABLE gold.dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id INTEGER UNIQUE NOT NULL,
            customer_name VARCHAR(200) NOT NULL,
            email_clean VARCHAR(200) NOT NULL,
            region_clean VARCHAR(50) NOT NULL,
            name_first VARCHAR(100),
            name_last VARCHAR(100),
            email_domain VARCHAR(100),
            customer_tier VARCHAR(20),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Load customer dimension
        INSERT INTO gold.dim_customer (
            customer_id, customer_name, email_clean, region_clean, 
            name_first, name_last, email_domain, customer_tier
        )
        SELECT 
            customer_id,
            customer_name_clean,
            email_clean,
            region_clean,
            name_first,
            name_last,
            email_domain,
            CASE 
                WHEN customer_id % 10 = 0 THEN 'Premium'  -- 10% premium customers
                WHEN customer_id % 5 = 0 THEN 'Gold'      -- 20% gold customers  
                WHEN customer_id % 3 = 0 THEN 'Silver'    -- 33% silver customers
                ELSE 'Bronze'                              -- Rest bronze
            END as customer_tier
        FROM silver.customers_standardized;

        -- Sales Fact Table
        DROP TABLE IF EXISTS gold.fact_sales CASCADE;
        CREATE TABLE gold.fact_sales (
            sales_key SERIAL PRIMARY KEY,
            sales_id INTEGER UNIQUE NOT NULL,
            customer_key INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(12,2) NOT NULL,
            total_amount DECIMAL(12,2) NOT NULL,
            revenue_category VARCHAR(20),
            year_month INTEGER GENERATED ALWAYS AS (
                EXTRACT(YEAR FROM order_date) * 100 + EXTRACT(MONTH FROM order_date)
            ) STORED,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) 
                REFERENCES gold.dim_customer(customer_key),
            CONSTRAINT chk_positive_amounts CHECK (total_amount > 0 AND unit_price > 0),
            CONSTRAINT chk_positive_quantity CHECK (quantity > 0)
        );

        -- Load sales facts
        INSERT INTO gold.fact_sales (
            sales_id, customer_key, product_id, order_date, 
            quantity, unit_price, total_amount, revenue_category
        )
        SELECT 
            s.sales_id,
            dc.customer_key,
            s.product_id,
            s.sale_date,
            s.quantity,
            ROUND(s.sale_amount / s.quantity, 2) as unit_price,
            s.sale_amount,
            s.revenue_category
        FROM silver.sales_cleaned s
        JOIN gold.dim_customer dc ON dc.customer_id = s.customer_id;

        -- Create performance indexes
        CREATE INDEX idx_fact_sales_customer_key ON gold.fact_sales(customer_key);
        CREATE INDEX idx_fact_sales_product_id ON gold.fact_sales(product_id);
        CREATE INDEX idx_fact_sales_date ON gold.fact_sales(order_date);
        CREATE INDEX idx_fact_sales_year_month ON gold.fact_sales(year_month);
        CREATE INDEX idx_dim_customer_tier ON gold.dim_customer(customer_tier);
        CREATE INDEX idx_dim_customer_region ON gold.dim_customer(region_clean);
        ''',
        doc_md="Create production dimensional model with fact and dimension tables"
    )

    # Build analytics aggregation tables
    build_analytics_tables = SQLExecuteQueryOperator(
        task_id='build_analytics_aggregations',
        conn_id='postgres_default',
        sql='''
        -- Monthly Sales Summary Analytics
        DROP TABLE IF EXISTS gold.monthly_sales_summary CASCADE;
        CREATE TABLE gold.monthly_sales_summary AS
        SELECT 
            EXTRACT(YEAR FROM order_date) as sale_year,
            EXTRACT(MONTH FROM order_date) as sale_month,
            year_month,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_sale_amount,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT customer_key) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            MIN(order_date) as period_start,
            MAX(order_date) as period_end,
            CURRENT_TIMESTAMP as created_at
        FROM gold.fact_sales
        GROUP BY 1, 2, 3
        ORDER BY sale_year DESC, sale_month DESC;

        -- Regional Performance Analytics
        DROP TABLE IF EXISTS gold.regional_performance CASCADE;
        CREATE TABLE gold.regional_performance AS
        SELECT 
            dc.region_clean,
            COUNT(DISTINCT dc.customer_key) as total_customers,
            COUNT(fs.sales_key) as total_transactions,
            SUM(fs.total_amount) as total_revenue,
            AVG(fs.total_amount) as avg_transaction_value,
            SUM(fs.total_amount) / COUNT(DISTINCT dc.customer_key) as revenue_per_customer,
            CURRENT_TIMESTAMP as created_at
        FROM gold.dim_customer dc
        LEFT JOIN gold.fact_sales fs ON dc.customer_key = fs.customer_key
        GROUP BY dc.region_clean
        ORDER BY total_revenue DESC;

        -- Product Performance Analytics
        DROP TABLE IF EXISTS gold.product_performance CASCADE;
        CREATE TABLE gold.product_performance AS
        SELECT 
            product_id,
            COUNT(*) as total_sales,
            SUM(total_amount) as total_revenue,
            SUM(quantity) as total_quantity_sold,
            AVG(unit_price) as avg_sale_price,
            COUNT(DISTINCT customer_key) as unique_buyers,
            RANK() OVER (ORDER BY SUM(total_amount) DESC) as revenue_rank,
            CURRENT_TIMESTAMP as created_at
        FROM gold.fact_sales
        GROUP BY product_id
        ORDER BY total_revenue DESC;

        -- Customer Analytics Table
        DROP TABLE IF EXISTS gold.customer_analytics CASCADE;
        CREATE TABLE gold.customer_analytics AS
        SELECT 
            dc.customer_key,
            dc.customer_id,
            dc.customer_name,
            dc.region_clean,
            dc.customer_tier,
            COUNT(fs.sales_key) as total_orders,
            COALESCE(SUM(fs.total_amount), 0) as lifetime_value,
            COALESCE(AVG(fs.total_amount), 0) as avg_order_value,
            MIN(fs.order_date) as first_purchase,
            MAX(fs.order_date) as last_purchase,
            COUNT(DISTINCT fs.product_id) as unique_products_purchased,
            CASE 
                WHEN MAX(fs.order_date) >= CURRENT_DATE - INTERVAL '30 days' THEN 'Active'
                WHEN MAX(fs.order_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'Recent'
                WHEN MAX(fs.order_date) >= CURRENT_DATE - INTERVAL '365 days' THEN 'Dormant'
                ELSE 'Inactive'
            END as customer_status,
            CURRENT_TIMESTAMP as created_at
        FROM gold.dim_customer dc
        LEFT JOIN gold.fact_sales fs ON dc.customer_key = fs.customer_key
        GROUP BY dc.customer_key, dc.customer_id, dc.customer_name, dc.region_clean, dc.customer_tier;

        -- Create analytics indexes
        CREATE INDEX idx_monthly_summary_year_month ON gold.monthly_sales_summary(sale_year, sale_month);
        CREATE INDEX idx_regional_performance_revenue ON gold.regional_performance(total_revenue DESC);
        CREATE INDEX idx_product_performance_revenue ON gold.product_performance(total_revenue DESC);
        CREATE INDEX idx_customer_analytics_tier ON gold.customer_analytics(customer_tier);
        CREATE INDEX idx_customer_analytics_status ON gold.customer_analytics(customer_status);
        CREATE INDEX idx_customer_analytics_lifetime_value ON gold.customer_analytics(lifetime_value DESC);
        ''',
        doc_md="Build pre-aggregated analytics tables for fast BI queries"
    )

    # Create business intelligence views
    create_bi_views = SQLExecuteQueryOperator(
        task_id='create_bi_views',
        conn_id='postgres_default',
        sql='''
        -- Executive Dashboard View
        CREATE OR REPLACE VIEW gold.v_executive_dashboard AS
        SELECT 
            'Total Revenue' as metric,
            TO_CHAR(SUM(total_revenue), 'FM$999,999,999,990.00') as value,
            'Current Period' as period
        FROM gold.monthly_sales_summary
        UNION ALL
        SELECT 
            'Total Customers' as metric,
            COUNT(*)::TEXT as value,
            'Active' as period
        FROM gold.customer_analytics
        WHERE customer_status IN ('Active', 'Recent')
        UNION ALL
        SELECT 
            'Avg Order Value' as metric,
            TO_CHAR(AVG(avg_order_value), 'FM$999,999,990.00') as value,
            'All Time' as period
        FROM gold.customer_analytics
        WHERE total_orders > 0;

        -- Regional Performance Dashboard
        CREATE OR REPLACE VIEW gold.v_regional_dashboard AS
        SELECT 
            region_clean as region,
            total_customers,
            total_transactions,
            TO_CHAR(total_revenue, 'FM$999,999,999,990.00') as total_revenue_formatted,
            TO_CHAR(avg_transaction_value, 'FM$999,990.00') as avg_transaction_formatted,
            TO_CHAR(revenue_per_customer, 'FM$999,990.00') as revenue_per_customer_formatted,
            RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
        FROM gold.regional_performance
        ORDER BY total_revenue DESC;

        -- Customer Segmentation View
        CREATE OR REPLACE VIEW gold.v_customer_segmentation AS
        SELECT 
            customer_tier,
            customer_status,
            COUNT(*) as customer_count,
            SUM(lifetime_value) as tier_revenue,
            AVG(lifetime_value) as avg_lifetime_value,
            AVG(total_orders) as avg_orders_per_customer,
            AVG(unique_products_purchased) as avg_products_per_customer
        FROM gold.customer_analytics
        GROUP BY customer_tier, customer_status
        ORDER BY customer_tier, customer_status;

        -- Product Performance View
        CREATE OR REPLACE VIEW gold.v_product_performance AS
        SELECT 
            product_id,
            total_sales,
            TO_CHAR(total_revenue, 'FM$999,999,999,990.00') as total_revenue_formatted,
            total_quantity_sold,
            TO_CHAR(avg_sale_price, 'FM$999,990.00') as avg_price_formatted,
            unique_buyers,
            revenue_rank,
            CASE 
                WHEN revenue_rank <= 10 THEN 'Top 10'
                WHEN revenue_rank <= 50 THEN 'Top 50'
                WHEN revenue_rank <= 100 THEN 'Top 100'
                ELSE 'Standard'
            END as product_tier
        FROM gold.product_performance
        ORDER BY revenue_rank;

        -- Time Series Analysis View
        CREATE OR REPLACE VIEW gold.v_sales_trends AS
        SELECT 
            sale_year,
            sale_month,
            year_month,
            total_transactions,
            TO_CHAR(total_revenue, 'FM$999,999,999,990.00') as total_revenue_formatted,
            TO_CHAR(avg_sale_amount, 'FM$999,990.00') as avg_sale_formatted,
            unique_customers,
            unique_products,
            LAG(total_revenue) OVER (ORDER BY year_month) as prev_month_revenue,
            ROUND(
                CASE 
                    WHEN LAG(total_revenue) OVER (ORDER BY year_month) > 0 
                    THEN ((total_revenue - LAG(total_revenue) OVER (ORDER BY year_month)) 
                          / LAG(total_revenue) OVER (ORDER BY year_month) * 100)
                    ELSE 0
                END, 2
            ) as revenue_growth_percent
        FROM gold.monthly_sales_summary
        ORDER BY year_month DESC;
        ''',
        doc_md="Create business intelligence views for executive reporting"
    )

    # Validate gold layer quality
    validate_gold_quality = PythonOperator(
        task_id='validate_gold_layer_quality',
        python_callable=validate_gold_layer,
        doc_md="Validate gold layer dimensional model and business metrics"
    )

    # Generate BI report
    generate_bi_report = PythonOperator(
        task_id='generate_business_intelligence_report',
        python_callable=generate_business_intelligence_report,
        doc_md="Generate comprehensive business intelligence report"
    )

    # Set dependencies
    wait_for_silver >> create_dimensional_model >> build_analytics_tables >> create_bi_views >> validate_gold_quality >> generate_bi_report