-- MetaLayer Database Partitioning Strategy
-- Time-based partitioning for optimal query performance and maintenance

-- =====================================================
-- DROP EXISTING TABLES (FOR MIGRATION)
-- =====================================================
DROP TABLE IF EXISTS bronze.erp_orders_raw;
DROP TABLE IF EXISTS bronze.crm_customers_raw;
DROP TABLE IF EXISTS silver.orders;
DROP TABLE IF EXISTS silver.customers;
DROP TABLE IF EXISTS gold.fact_sales;
DROP TABLE IF EXISTS gold.dim_customer;

-- =====================================================
-- BRONZE LAYER - PARTITIONED TABLES
-- =====================================================

-- Orders table with monthly partitioning by order_date
CREATE TABLE bronze.erp_orders_raw (
    order_id        VARCHAR(50) NOT NULL,
    customer_id     VARCHAR(50) NOT NULL,
    product_code    VARCHAR(50),
    quantity        INTEGER,
    price_per_unit  DECIMAL(18,2),
    order_date      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Partition key constraint
    CONSTRAINT erp_orders_raw_pkey PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

-- Create indexes for partitioned table
CREATE INDEX idx_erp_orders_raw_customer_id ON bronze.erp_orders_raw (customer_id);
CREATE INDEX idx_erp_orders_raw_product_code ON bronze.erp_orders_raw (product_code);
CREATE INDEX idx_erp_orders_raw_order_date ON bronze.erp_orders_raw (order_date);

-- Customers table with yearly partitioning by created_date
CREATE TABLE bronze.crm_customers_raw (
    customer_id     VARCHAR(50) NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    phone           VARCHAR(50),
    address         VARCHAR(500),
    city            VARCHAR(100),
    state           VARCHAR(50),
    zip             VARCHAR(20),
    dob             DATE,
    created_date    TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Partition key constraint
    CONSTRAINT crm_customers_raw_pkey PRIMARY KEY (customer_id, created_date)
) PARTITION BY RANGE (created_date);

-- Create indexes for customers
CREATE INDEX idx_crm_customers_email ON bronze.crm_customers_raw (email);
CREATE INDEX idx_crm_customers_created_date ON bronze.crm_customers_raw (created_date);
CREATE INDEX idx_crm_customers_state ON bronze.crm_customers_raw (state);

-- =====================================================
-- SILVER LAYER - PARTITIONED TABLES
-- =====================================================

-- Silver orders with monthly partitioning
CREATE TABLE silver.orders (
    order_id        VARCHAR(50) NOT NULL,
    customer_id     VARCHAR(50) NOT NULL,
    product_code    VARCHAR(50),
    quantity        INTEGER CHECK (quantity > 0),
    price_per_unit  DECIMAL(18,2) CHECK (price_per_unit > 0),
    total_amount    DECIMAL(18,2) GENERATED ALWAYS AS (quantity * price_per_unit) STORED,
    order_date      DATE NOT NULL,
    order_status    VARCHAR(30) DEFAULT 'PENDING',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Data quality flags
    is_valid        BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    -- Partition key constraint  
    CONSTRAINT silver_orders_pkey PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

-- Indexes for silver orders
CREATE INDEX idx_silver_orders_customer_id ON silver.orders (customer_id);
CREATE INDEX idx_silver_orders_status ON silver.orders (order_status);
CREATE INDEX idx_silver_orders_total_amount ON silver.orders (total_amount);

-- Silver customers with yearly partitioning
CREATE TABLE silver.customers (
    customer_id     VARCHAR(50) NOT NULL,
    customer_name   VARCHAR(200),
    email_clean     VARCHAR(200),
    phone_clean     VARCHAR(50),
    address_standardized VARCHAR(500),
    city_standardized    VARCHAR(100),
    state_code      VARCHAR(10),
    zip_clean       VARCHAR(20),
    date_of_birth   DATE,
    customer_since  DATE NOT NULL,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Customer segmentation
    customer_tier   VARCHAR(20) DEFAULT 'STANDARD',
    lifetime_value  DECIMAL(18,2) DEFAULT 0.00,
    -- Data quality flags
    is_valid        BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    -- Partition key constraint
    CONSTRAINT silver_customers_pkey PRIMARY KEY (customer_id, customer_since)
) PARTITION BY RANGE (customer_since);

-- Indexes for silver customers
CREATE INDEX idx_silver_customers_email ON silver.customers (email_clean);
CREATE INDEX idx_silver_customers_tier ON silver.customers (customer_tier);
CREATE INDEX idx_silver_customers_lifetime_value ON silver.customers (lifetime_value);

-- =====================================================
-- GOLD LAYER - DIMENSIONAL MODEL WITH PARTITIONING
-- =====================================================

-- Dimension Customer (SCD Type 2 with partitioning)
CREATE TABLE gold.dim_customer (
    customer_key    SERIAL,
    customer_id     VARCHAR(50) NOT NULL,
    customer_name   VARCHAR(200),
    email           VARCHAR(200),
    phone           VARCHAR(50),
    address         VARCHAR(500),
    city            VARCHAR(100),
    state_code      VARCHAR(10),
    zip_code        VARCHAR(20),
    customer_tier   VARCHAR(20),
    -- SCD Type 2 fields
    effective_date  DATE NOT NULL,
    expiry_date     DATE DEFAULT '2099-12-31',
    is_current      BOOLEAN DEFAULT TRUE,
    version_number  INTEGER DEFAULT 1,
    -- Audit fields
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Partition key constraint
    CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_key, effective_date)
) PARTITION BY RANGE (effective_date);

-- Indexes for dimension customer
CREATE UNIQUE INDEX idx_dim_customer_id_current ON gold.dim_customer (customer_id) WHERE is_current = TRUE;
CREATE INDEX idx_dim_customer_effective_date ON gold.dim_customer (effective_date);
CREATE INDEX idx_dim_customer_tier ON gold.dim_customer (customer_tier);

-- Fact Sales with monthly partitioning
CREATE TABLE gold.fact_sales (
    sales_key       SERIAL,
    order_id        VARCHAR(50) NOT NULL,
    customer_key    INTEGER NOT NULL,
    product_code    VARCHAR(50),
    order_date      DATE NOT NULL,
    quantity        INTEGER CHECK (quantity > 0),
    unit_price      DECIMAL(18,2) CHECK (unit_price > 0),
    total_amount    DECIMAL(18,2) CHECK (total_amount > 0),
    order_status    VARCHAR(30),
    -- Time dimension keys
    year_month      INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM order_date) * 100 + EXTRACT(MONTH FROM order_date)) STORED,
    quarter         INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM order_date)) STORED,
    day_of_week     INTEGER GENERATED ALWAYS AS (EXTRACT(DOW FROM order_date)) STORED,
    -- Audit fields
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Partition key constraint
    CONSTRAINT fact_sales_pkey PRIMARY KEY (sales_key, order_date)
) PARTITION BY RANGE (order_date);

-- Indexes for fact sales
CREATE INDEX idx_fact_sales_customer_key ON gold.fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product_code ON gold.fact_sales (product_code);
CREATE INDEX idx_fact_sales_year_month ON gold.fact_sales (year_month);
CREATE INDEX idx_fact_sales_total_amount ON gold.fact_sales (total_amount);

-- =====================================================
-- AUTOMATIC PARTITION MANAGEMENT FUNCTIONS
-- =====================================================

-- Function to create monthly partitions for orders/sales
CREATE OR REPLACE FUNCTION create_monthly_partition(
    parent_table TEXT,
    partition_date DATE
) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
BEGIN
    -- Generate partition name (e.g., orders_2024_01)
    partition_name := parent_table || '_' || 
                     TO_CHAR(partition_date, 'YYYY_MM');
    
    -- Calculate partition bounds
    start_date := TO_CHAR(DATE_TRUNC('month', partition_date), 'YYYY-MM-DD');
    end_date := TO_CHAR(DATE_TRUNC('month', partition_date) + INTERVAL '1 month', 'YYYY-MM-DD');
    
    -- Create partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I 
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, parent_table, start_date, end_date
    );
    
    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to create yearly partitions for customers
CREATE OR REPLACE FUNCTION create_yearly_partition(
    parent_table TEXT,
    partition_date DATE
) RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_date TEXT;
    end_date TEXT;
BEGIN
    -- Generate partition name (e.g., customers_2024)
    partition_name := parent_table || '_' || 
                     TO_CHAR(partition_date, 'YYYY');
    
    -- Calculate partition bounds
    start_date := TO_CHAR(DATE_TRUNC('year', partition_date), 'YYYY-MM-DD');
    end_date := TO_CHAR(DATE_TRUNC('year', partition_date) + INTERVAL '1 year', 'YYYY-MM-DD');
    
    -- Create partition
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I 
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, parent_table, start_date, end_date
    );
    
    RETURN partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to auto-create partitions based on data
CREATE OR REPLACE FUNCTION auto_create_partitions()
RETURNS void AS $$
DECLARE
    start_date DATE;
    end_date DATE;
    current_date_iter DATE;
BEGIN
    -- Determine date range from existing data
    SELECT 
        DATE_TRUNC('month', MIN(order_date))::DATE,
        DATE_TRUNC('month', MAX(order_date) + INTERVAL '2 months')::DATE
    INTO start_date, end_date
    FROM (
        SELECT CURRENT_DATE - INTERVAL '1 year' as order_date
        UNION ALL
        SELECT CURRENT_DATE + INTERVAL '1 year' as order_date
    ) date_range;
    
    -- Create monthly partitions for order tables
    current_date_iter := start_date;
    WHILE current_date_iter < end_date LOOP
        -- Bronze orders
        PERFORM create_monthly_partition('bronze.erp_orders_raw', current_date_iter);
        -- Silver orders  
        PERFORM create_monthly_partition('silver.orders', current_date_iter);
        -- Gold fact sales
        PERFORM create_monthly_partition('gold.fact_sales', current_date_iter);
        
        current_date_iter := current_date_iter + INTERVAL '1 month';
    END LOOP;
    
    -- Create yearly partitions for customer tables
    current_date_iter := DATE_TRUNC('year', start_date);
    WHILE current_date_iter < DATE_TRUNC('year', end_date) + INTERVAL '1 year' LOOP
        -- Bronze customers
        PERFORM create_yearly_partition('bronze.crm_customers_raw', current_date_iter);
        -- Silver customers
        PERFORM create_yearly_partition('silver.customers', current_date_iter);
        -- Gold dim customer
        PERFORM create_yearly_partition('gold.dim_customer', current_date_iter);
        
        current_date_iter := current_date_iter + INTERVAL '1 year';
    END LOOP;
    
    RAISE NOTICE 'Partition creation completed for date range % to %', start_date, end_date;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- PARTITION MAINTENANCE PROCEDURES
-- =====================================================

-- Function to drop old partitions (for data retention)
CREATE OR REPLACE FUNCTION cleanup_old_partitions(
    retention_months INTEGER DEFAULT 24
) RETURNS INTEGER AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE;
    dropped_count INTEGER := 0;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '1 month' * retention_months;
    
    -- Find and drop old partitions
    FOR partition_record IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname IN ('bronze', 'silver', 'gold')
        AND tablename ~ '_\d{4}_\d{2}$|_\d{4}$'
        AND (
            -- Monthly partitions older than cutoff
            (tablename ~ '_\d{4}_\d{2}$' AND 
             TO_DATE(RIGHT(tablename, 7), 'YYYY_MM') < cutoff_date)
            OR
            -- Yearly partitions older than cutoff
            (tablename ~ '_\d{4}$' AND 
             TO_DATE(RIGHT(tablename, 4) || '-01-01', 'YYYY-MM-DD') < cutoff_date)
        )
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I.%I', 
                      partition_record.schemaname, 
                      partition_record.tablename);
        dropped_count := dropped_count + 1;
        
        RAISE NOTICE 'Dropped old partition: %.%', 
                     partition_record.schemaname, 
                     partition_record.tablename;
    END LOOP;
    
    RETURN dropped_count;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- INITIAL PARTITION SETUP
-- =====================================================

-- Create initial partitions
SELECT auto_create_partitions();

-- Verify partition creation
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('bronze', 'silver', 'gold')
ORDER BY schemaname, tablename;