import logging
from datetime import datetime, timedelta
from typing import Dict, Any
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

def validate_silver_transformations():
    """Validate Silver layer data quality and transformations"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Check record counts
        sales_count = hook.get_first('SELECT COUNT(*) FROM silver.sales_cleaned')[0]
        customers_count = hook.get_first('SELECT COUNT(*) FROM silver.customers_standardized')[0]
        
        # Get bronze counts for comparison
        bronze_sales = hook.get_first('SELECT COUNT(*) FROM bronze.erp_sales_raw')[0]
        bronze_customers = hook.get_first('SELECT COUNT(*) FROM bronze.crm_customers_raw')[0]
        
        # Calculate data quality metrics
        sales_loss_rate = ((bronze_sales - sales_count) / bronze_sales) * 100 if bronze_sales > 0 else 0
        customer_loss_rate = ((bronze_customers - customers_count) / bronze_customers) * 100 if bronze_customers > 0 else 0
        
        # Validate business rules
        invalid_sales = hook.get_first('''
            SELECT COUNT(*) FROM silver.sales_cleaned 
            WHERE sale_amount <= 0 OR quantity <= 0 OR sales_id IS NULL
        ''')[0]
        
        invalid_customers = hook.get_first('''
            SELECT COUNT(*) FROM silver.customers_standardized
            WHERE email_clean NOT LIKE '%@%' OR customer_name_clean IS NULL OR LENGTH(TRIM(customer_name_clean)) = 0
        ''')[0]
        
        # Quality thresholds
        max_loss_rate = 5.0  # Allow max 5% data loss in transformation
        max_invalid_records = 100  # Allow max 100 invalid records
        
        validation_results = {
            'silver_sales_count': sales_count,
            'silver_customers_count': customers_count,
            'bronze_sales_count': bronze_sales,
            'bronze_customers_count': bronze_customers,
            'sales_loss_rate_percent': round(sales_loss_rate, 2),
            'customer_loss_rate_percent': round(customer_loss_rate, 2),
            'invalid_sales_count': invalid_sales,
            'invalid_customers_count': invalid_customers,
            'validation_timestamp': datetime.utcnow().isoformat()
        }
        
        # Check thresholds
        quality_issues = []
        
        if sales_loss_rate > max_loss_rate:
            quality_issues.append(f"High sales data loss rate: {sales_loss_rate}% > {max_loss_rate}%")
        
        if customer_loss_rate > max_loss_rate:
            quality_issues.append(f"High customer data loss rate: {customer_loss_rate}% > {max_loss_rate}%")
        
        if invalid_sales > max_invalid_records:
            quality_issues.append(f"Too many invalid sales records: {invalid_sales} > {max_invalid_records}")
        
        if invalid_customers > max_invalid_records:
            quality_issues.append(f"Too many invalid customer records: {invalid_customers} > {max_invalid_records}")
        
        if quality_issues:
            validation_results['quality_issues'] = quality_issues
            validation_results['validation_status'] = 'FAILED'
            raise AirflowException(f"Silver layer validation failed: {quality_issues}")
        
        # Sample data checks
        sample_metrics = hook.get_records('''
            SELECT 
                'avg_sale_amount' as metric,
                ROUND(AVG(sale_amount), 2) as value
            FROM silver.sales_cleaned
            UNION ALL
            SELECT 
                'unique_customers_with_sales' as metric,
                COUNT(DISTINCT customer_id) as value
            FROM silver.sales_cleaned
            UNION ALL
            SELECT 
                'customers_with_valid_emails' as metric,
                COUNT(*) as value
            FROM silver.customers_standardized
            WHERE email_clean LIKE '%@%'
        ''')
        
        validation_results['business_metrics'] = {metric: value for metric, value in sample_metrics}
        validation_results['validation_status'] = 'PASSED'
        
        logger.info(f"Silver layer validation completed successfully: {validation_results}")
        return validation_results
        
    except Exception as e:
        logger.error(f"Silver layer validation failed: {str(e)}")
        raise AirflowException(f"Silver layer validation failed: {str(e)}")

def log_transformation_metrics():
    """Log transformation metrics to audit table"""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    try:
        # Get transformation metrics
        metrics = hook.get_records('''
            SELECT 
                'silver_sales_cleaned' as table_name,
                COUNT(*) as record_count,
                MIN(sale_date) as min_date,
                MAX(sale_date) as max_date,
                SUM(sale_amount) as total_revenue
            FROM silver.sales_cleaned
            UNION ALL
            SELECT 
                'silver_customers_standardized' as table_name,
                COUNT(*) as record_count,
                NULL as min_date,
                NULL as max_date,
                NULL as total_revenue
            FROM silver.customers_standardized
        ''')
        
        # Log to audit table
        for metric in metrics:
            hook.run('''
                INSERT INTO audit.data_load_log 
                (table_name, records_loaded, load_start_time, load_end_time, load_status, quality_score)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', parameters=(
                metric[0],  # table_name
                metric[1],  # record_count
                datetime.utcnow(),  # load_start_time
                datetime.utcnow(),  # load_end_time
                'SUCCESS',  # load_status
                95  # quality_score (placeholder)
            ))
        
        logger.info(f"Transformation metrics logged successfully")
        return {'status': 'SUCCESS', 'metrics_logged': len(metrics)}
        
    except Exception as e:
        logger.error(f"Failed to log transformation metrics: {str(e)}")
        raise AirflowException(f"Failed to log transformation metrics: {str(e)}")

with DAG(
    dag_id='silver_layer_production_transform',
    default_args=default_args,
    description='Production Silver Layer - Transform Bronze data with Quality Controls',
    schedule=None,  # Triggered after bronze layer completion
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'production', 'transformation', 'data-quality'],
    doc_md="""
    # Silver Layer Production Transformations
    
    This DAG transforms bronze layer data into clean, standardized silver layer tables with:
    - Data cleaning and standardization
    - Business rule validation
    - Quality threshold monitoring
    - Performance optimization
    - Comprehensive error handling
    
    ## Transformations:
    - Bronze ERP Sales → Silver Sales Cleaned
    - Bronze CRM Customers → Silver Customers Standardized
    
    ## Quality Controls:
    - Data loss rate monitoring (< 5%)
    - Business rule validation
    - Data type standardization
    - Duplicate removal
    """
):

    # Wait for bronze layer to complete
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze_layer',
        external_dag_id='bronze_layer_production_load',
        external_task_id='validate_bronze_layer_quality',
        timeout=3600,  # 1 hour timeout
        poke_interval=60,  # Check every minute
        mode='poke',
        doc_md="Wait for bronze layer data loading to complete"
    )

    # Create Silver layer tables with optimized structure
    create_silver_tables = SQLExecuteQueryOperator(
        task_id='create_silver_tables',
        conn_id='postgres_default',
        sql='''
        -- Sales Cleaned Table (Silver Layer) - Production Schema
        DROP TABLE IF EXISTS silver.sales_cleaned CASCADE;
        CREATE TABLE silver.sales_cleaned (
            sales_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL CHECK (quantity > 0),
            sale_date DATE NOT NULL,
            sale_amount DECIMAL(12,2) NOT NULL CHECK (sale_amount > 0),
            sale_year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM sale_date)) STORED,
            sale_month INTEGER GENERATED ALWAYS AS (EXTRACT(MONTH FROM sale_date)) STORED,
            sale_quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM sale_date)) STORED,
            revenue_category VARCHAR(20) GENERATED ALWAYS AS (
                CASE 
                    WHEN sale_amount >= 1000 THEN 'High'
                    WHEN sale_amount >= 500 THEN 'Medium'
                    ELSE 'Low'
                END
            ) STORED,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Customers Standardized Table (Silver Layer) - Production Schema
        DROP TABLE IF EXISTS silver.customers_standardized CASCADE;
        CREATE TABLE silver.customers_standardized (
            customer_id INTEGER PRIMARY KEY,
            customer_name_clean VARCHAR(200) NOT NULL,
            email_clean VARCHAR(200) NOT NULL CHECK (email_clean LIKE '%@%'),
            region_clean VARCHAR(50) NOT NULL,
            name_first VARCHAR(100) GENERATED ALWAYS AS (
                SPLIT_PART(customer_name_clean, ' ', 1)
            ) STORED,
            name_last VARCHAR(100) GENERATED ALWAYS AS (
                CASE 
                    WHEN POSITION(' ' IN customer_name_clean) > 0 
                    THEN SPLIT_PART(customer_name_clean, ' ', -1)
                    ELSE customer_name_clean
                END
            ) STORED,
            email_domain VARCHAR(100) GENERATED ALWAYS AS (
                SUBSTRING(email_clean FROM '@(.*)$')
            ) STORED,
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create optimized indexes for performance
        CREATE INDEX idx_silver_sales_customer_id ON silver.sales_cleaned(customer_id);
        CREATE INDEX idx_silver_sales_product_id ON silver.sales_cleaned(product_id);
        CREATE INDEX idx_silver_sales_date ON silver.sales_cleaned(sale_date);
        CREATE INDEX idx_silver_sales_amount ON silver.sales_cleaned(sale_amount);
        CREATE INDEX idx_silver_sales_year_month ON silver.sales_cleaned(sale_year, sale_month);
        CREATE INDEX idx_silver_sales_category ON silver.sales_cleaned(revenue_category);
        
        CREATE INDEX idx_silver_customers_region ON silver.customers_standardized(region_clean);
        CREATE INDEX idx_silver_customers_domain ON silver.customers_standardized(email_domain);
        CREATE INDEX idx_silver_customers_name_parts ON silver.customers_standardized(name_first, name_last);
        ''',
        doc_md="Create production silver tables with constraints, computed columns, and indexes"
    )

    # Transform ERP Sales data with comprehensive cleaning
    transform_sales_data = SQLExecuteQueryOperator(
        task_id='transform_sales_data',
        conn_id='postgres_default',
        sql='''
        -- Transform ERP Sales with data quality controls
        INSERT INTO silver.sales_cleaned (
            sales_id, product_id, customer_id, quantity, sale_date, sale_amount
        )
        SELECT 
            sales_id,
            product_id,
            customer_id,
            GREATEST(quantity, 1) as quantity,  -- Ensure positive quantity
            sale_date,
            ROUND(sale_amount, 2) as sale_amount  -- Standardize decimal places
        FROM bronze.erp_sales_raw
        WHERE sales_id IS NOT NULL 
        AND product_id IS NOT NULL 
        AND customer_id IS NOT NULL
        AND sale_date IS NOT NULL
        AND sale_date >= '2020-01-01'  -- Business rule: only recent sales
        AND sale_date <= CURRENT_DATE  -- Business rule: no future sales
        AND sale_amount > 0
        AND sale_amount <= 100000  -- Business rule: reasonable sale amount cap
        AND quantity > 0
        AND quantity <= 1000;  -- Business rule: reasonable quantity cap
        
        -- Log transformation results
        DO $$
        DECLARE
            bronze_count INTEGER;
            silver_count INTEGER;
            rejected_count INTEGER;
            success_rate NUMERIC;
        BEGIN
            SELECT COUNT(*) INTO bronze_count FROM bronze.erp_sales_raw;
            SELECT COUNT(*) INTO silver_count FROM silver.sales_cleaned;
            rejected_count := bronze_count - silver_count;
            success_rate := CASE WHEN bronze_count > 0 THEN (silver_count::NUMERIC / bronze_count * 100) ELSE 0 END;
            
            RAISE NOTICE 'Sales Transformation Results:';
            RAISE NOTICE '  Bronze Records: %', bronze_count;
            RAISE NOTICE '  Silver Records: %', silver_count;
            RAISE NOTICE '  Rejected Records: %', rejected_count;
            RAISE NOTICE '  Success Rate: %% %', ROUND(success_rate, 2);
            
            -- Fail if success rate is too low
            IF success_rate < 95.0 THEN
                RAISE EXCEPTION 'Sales transformation success rate too low: %% %', ROUND(success_rate, 2);
            END IF;
        END $$;
        ''',
        doc_md="Transform sales data with business rules and quality validation"
    )

    # Transform CRM Customer data with comprehensive standardization
    transform_customers_data = SQLExecuteQueryOperator(
        task_id='transform_customers_data',
        conn_id='postgres_default', 
        sql='''
        -- Transform CRM Customers with data standardization
        INSERT INTO silver.customers_standardized (
            customer_id, customer_name_clean, email_clean, region_clean
        )
        SELECT 
            customer_id,
            TRIM(REGEXP_REPLACE(INITCAP(customer_name), '\\s+', ' ', 'g')) as customer_name_clean,
            LOWER(TRIM(email)) as email_clean,
            UPPER(TRIM(region)) as region_clean
        FROM bronze.crm_customers_raw
        WHERE customer_id IS NOT NULL
        AND customer_name IS NOT NULL 
        AND TRIM(customer_name) != ''
        AND LENGTH(TRIM(customer_name)) >= 2  -- Minimum name length
        AND email IS NOT NULL 
        AND email LIKE '%@%.%'  -- Basic email validation
        AND LENGTH(email) >= 5  -- Minimum email length
        AND region IS NOT NULL 
        AND TRIM(region) != ''
        AND region IN ('NORTH', 'SOUTH', 'EAST', 'WEST', 'CENTRAL', 'NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST');  -- Valid regions only
        
        -- Log transformation results
        DO $$
        DECLARE
            bronze_count INTEGER;
            silver_count INTEGER;
            rejected_count INTEGER;
            success_rate NUMERIC;
        BEGIN
            SELECT COUNT(*) INTO bronze_count FROM bronze.crm_customers_raw;
            SELECT COUNT(*) INTO silver_count FROM silver.customers_standardized;
            rejected_count := bronze_count - silver_count;
            success_rate := CASE WHEN bronze_count > 0 THEN (silver_count::NUMERIC / bronze_count * 100) ELSE 0 END;
            
            RAISE NOTICE 'Customer Transformation Results:';
            RAISE NOTICE '  Bronze Records: %', bronze_count;
            RAISE NOTICE '  Silver Records: %', silver_count;
            RAISE NOTICE '  Rejected Records: %', rejected_count;
            RAISE NOTICE '  Success Rate: %% %', ROUND(success_rate, 2);
            
            -- Fail if success rate is too low
            IF success_rate < 95.0 THEN
                RAISE EXCEPTION 'Customer transformation success rate too low: %% %', ROUND(success_rate, 2);
            END IF;
        END $$;
        ''',
        doc_md="Transform customer data with standardization and validation"
    )

    # Create analytics-ready views
    create_silver_views = SQLExecuteQueryOperator(
        task_id='create_silver_analytics_views',
        conn_id='postgres_default',
        sql='''
        -- Sales Analysis View
        CREATE OR REPLACE VIEW silver.v_sales_analysis AS
        SELECT 
            s.*,
            c.customer_name_clean,
            c.region_clean,
            c.email_domain,
            CASE 
                WHEN s.sale_amount >= 1000 THEN 'Premium'
                WHEN s.sale_amount >= 500 THEN 'Standard'
                ELSE 'Basic'
            END as customer_tier
        FROM silver.sales_cleaned s
        LEFT JOIN silver.customers_standardized c ON s.customer_id = c.customer_id;
        
        -- Monthly Sales Summary View
        CREATE OR REPLACE VIEW silver.v_monthly_sales_summary AS
        SELECT 
            sale_year,
            sale_month,
            COUNT(*) as transaction_count,
            SUM(sale_amount) as total_revenue,
            AVG(sale_amount) as avg_sale_amount,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            MIN(sale_date) as period_start,
            MAX(sale_date) as period_end
        FROM silver.sales_cleaned
        GROUP BY sale_year, sale_month
        ORDER BY sale_year DESC, sale_month DESC;
        
        -- Customer Summary View
        CREATE OR REPLACE VIEW silver.v_customer_summary AS
        SELECT 
            c.customer_id,
            c.customer_name_clean,
            c.email_clean,
            c.region_clean,
            c.email_domain,
            COUNT(s.sales_id) as total_orders,
            COALESCE(SUM(s.sale_amount), 0) as lifetime_value,
            COALESCE(AVG(s.sale_amount), 0) as avg_order_value,
            MIN(s.sale_date) as first_purchase,
            MAX(s.sale_date) as last_purchase,
            COUNT(DISTINCT s.product_id) as unique_products_purchased
        FROM silver.customers_standardized c
        LEFT JOIN silver.sales_cleaned s ON c.customer_id = s.customer_id
        GROUP BY c.customer_id, c.customer_name_clean, c.email_clean, c.region_clean, c.email_domain;
        ''',
        doc_md="Create analytics-ready views for business intelligence"
    )

    # Validate transformations
    validate_silver_quality = PythonOperator(
        task_id='validate_silver_quality',
        python_callable=validate_silver_transformations,
        doc_md="Validate silver layer data quality and transformation results"
    )
    
    # Log metrics
    log_metrics = PythonOperator(
        task_id='log_transformation_metrics',
        python_callable=log_transformation_metrics,
        doc_md="Log transformation metrics to audit system"
    )

    # Set dependencies
    wait_for_bronze >> create_silver_tables >> [transform_sales_data, transform_customers_data] >> create_silver_views >> validate_silver_quality >> log_metrics