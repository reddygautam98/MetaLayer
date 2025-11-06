# METALAYER ETL PIPELINE - COMPREHENSIVE TESTING GUIDE
===============================================================

## 🧪 COMPLETE TESTING PROCEDURE

This guide provides step-by-step instructions to test the complete MetaLayer ETL pipeline
from initial deployment through full end-to-end data processing validation.

## 🚀 PHASE 1: DEPLOYMENT VALIDATION

### Step 1: Start Docker Stack
```powershell
# Navigate to project directory
cd "C:\Users\reddy\Downloads\MetaLayer"

# Start all services
docker-compose up -d

# Verify all services are running
docker-compose ps
```

**Expected Output:**
```
NAME                    COMMAND                  SERVICE      STATUS
metalayer-airflow-init  "/usr/bin/dumb-init …"   airflow-init   Exit 0
metalayer-flower-1      "/usr/bin/dumb-init …"   flower         Up
metalayer-grafana-1     "/run.sh"                grafana        Up
metalayer-postgres-1    "docker-entrypoint.s…"   postgres       Up (healthy)
metalayer-prometheus-1  "/bin/prometheus --c…"   prometheus     Up
metalayer-redis-1       "docker-entrypoint.s…"   redis          Up (healthy)
metalayer-scheduler-1   "/usr/bin/dumb-init …"   scheduler      Up (healthy)
metalayer-webserver-1   "/usr/bin/dumb-init …"   webserver      Up (healthy)
metalayer-worker-1      "/usr/bin/dumb-init …"   worker         Up (healthy)
```

### Step 2: Service Health Verification
```powershell
# Check PostgreSQL connectivity
docker-compose exec postgres pg_isready -U airflow

# Check Redis connectivity
docker-compose exec redis redis-cli ping

# Check Airflow webserver
curl http://localhost:8080/health

# Check Grafana
curl http://localhost:3000/api/health
```

### Step 3: Database Schema Initialization
```powershell
# Initialize database schema
docker-compose exec postgres psql -U airflow -d airflow -f /opt/airflow/include/sql/complete_schema_docker.sql

# Verify schema creation
docker-compose exec postgres psql -U airflow -d airflow -c "\dn"
```

**Expected Schemas:**
- bronze
- silver  
- gold
- airflow_meta

## 🔧 PHASE 2: AIRFLOW CONFIGURATION

### Step 1: Setup Connections and Variables
```powershell
# Run configuration script
docker-compose exec webserver python /opt/airflow/scripts/setup_airflow_config.py

# Verify connections created
docker-compose exec webserver airflow connections list

# Verify variables created  
docker-compose exec webserver airflow variables list
```

### Step 2: DAG Import Validation
```powershell
# Check DAG imports
docker-compose exec webserver airflow dags list

# Verify DAG syntax
docker-compose exec webserver python -m py_compile /opt/airflow/dags/00_master_orchestrator_docker.py
docker-compose exec webserver python -m py_compile /opt/airflow/dags/01_bronze_layer_docker.py
docker-compose exec webserver python -m py_compile /opt/airflow/dags/02_silver_layer_docker.py
docker-compose exec webserver python -m py_compile /opt/airflow/dags/03_gold_layer_docker.py
```

**Expected DAGs:**
- master_etl_orchestrator
- bronze_layer_etl_pipeline
- silver_layer_etl_pipeline
- gold_layer_analytics_pipeline

### Step 3: Access UI Interfaces
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Flower**: http://localhost:5555

## 📊 PHASE 3: DATA LAYER TESTING

### Bronze Layer Testing

#### Test 1: Individual Task Testing
```powershell
# Test source data validation
docker-compose exec webserver airflow tasks test bronze_layer_etl_pipeline validate_source_data 2024-01-20

# Test CRM data ingestion
docker-compose exec webserver airflow tasks test bronze_layer_etl_pipeline ingest_crm_data 2024-01-20

# Test ERP data ingestion  
docker-compose exec webserver airflow tasks test bronze_layer_etl_pipeline ingest_erp_data 2024-01-20

# Test data quality validation
docker-compose exec webserver airflow tasks test bronze_layer_etl_pipeline validate_bronze_quality 2024-01-20
```

#### Test 2: Full Bronze Layer Execution
```powershell
# Trigger complete Bronze layer DAG
docker-compose exec webserver airflow dags trigger bronze_layer_etl_pipeline --execution-date 2024-01-20

# Monitor execution
docker-compose exec webserver airflow dags state bronze_layer_etl_pipeline 2024-01-20
```

#### Test 3: Bronze Data Verification
```sql
-- Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

-- Check Bronze layer data
SELECT COUNT(*) as customer_count FROM bronze.customers_raw;
SELECT COUNT(*) as order_count FROM bronze.orders_raw;
SELECT validation_status, COUNT(*) FROM bronze.customers_raw GROUP BY validation_status;
SELECT source_system, COUNT(*) FROM bronze.orders_raw GROUP BY source_system;
```

### Silver Layer Testing

#### Test 1: Individual Task Testing
```powershell
# Test customer transformation
docker-compose exec webserver airflow tasks test silver_layer_etl_pipeline transform_customers 2024-01-20

# Test order transformation
docker-compose exec webserver airflow tasks test silver_layer_etl_pipeline transform_orders 2024-01-20

# Test quality validation
docker-compose exec webserver airflow tasks test silver_layer_etl_pipeline validate_silver_quality 2024-01-20
```

#### Test 2: Silver Data Verification
```sql
-- Check Silver layer transformations
SELECT COUNT(*) as clean_customers FROM silver.customers_cleaned;
SELECT COUNT(*) as clean_orders FROM silver.orders_cleaned;

-- Verify data quality scores
SELECT AVG(data_quality_score) as avg_customer_quality FROM silver.customers_cleaned;
SELECT COUNT(*) FILTER (WHERE is_valid = true) as valid_orders FROM silver.orders_cleaned;

-- Check business rule application
SELECT is_active, COUNT(*) FROM silver.customers_cleaned GROUP BY is_active;
SELECT COUNT(*) FILTER (WHERE total_amount > 100) as high_value_orders FROM silver.orders_cleaned;
```

### Gold Layer Testing

#### Test 1: Dimensional Model Testing
```powershell
# Test customer dimension build
docker-compose exec webserver airflow tasks test gold_layer_analytics_pipeline build_customer_dimension 2024-01-20

# Test order fact table build
docker-compose exec webserver airflow tasks test gold_layer_analytics_pipeline build_order_fact_table 2024-01-20

# Test KPI calculations
docker-compose exec webserver airflow tasks test gold_layer_analytics_pipeline calculate_business_kpis 2024-01-20
```

#### Test 2: Analytics Verification
```sql
-- Check dimensional model
SELECT COUNT(*) as total_customers FROM gold.dim_customer;
SELECT COUNT(*) as current_customers FROM gold.dim_customer WHERE is_current = true;

-- Verify fact table
SELECT COUNT(*) as total_orders FROM gold.fact_orders;
SELECT SUM(total_amount) as total_revenue FROM gold.fact_orders WHERE is_valid = true;

-- Check aggregations
SELECT * FROM gold.agg_daily_sales WHERE date_key = 20240120;
SELECT * FROM gold.business_kpis WHERE date_key = 20240120;
```

## 🎯 PHASE 4: END-TO-END PIPELINE TESTING

### Full Pipeline Execution Test
```powershell
# Trigger master orchestrator
docker-compose exec webserver airflow dags trigger master_etl_orchestrator --execution-date 2024-01-20

# Monitor pipeline progress
watch -n 30 "docker-compose exec webserver airflow dags state master_etl_orchestrator 2024-01-20"

# Check individual layer completion
docker-compose exec webserver airflow dags state bronze_layer_etl_pipeline 2024-01-20
docker-compose exec webserver airflow dags state silver_layer_etl_pipeline 2024-01-20  
docker-compose exec webserver airflow dags state gold_layer_analytics_pipeline 2024-01-20
```

### Pipeline Metrics Validation
```sql
-- Check pipeline execution metrics
SELECT 
    dag_id,
    execution_date,
    status,
    total_duration_seconds,
    total_records,
    quality_score,
    sla_met
FROM airflow_meta.pipeline_runs 
WHERE execution_date::date = '2024-01-20'
ORDER BY dag_id;

-- Verify data lineage
SELECT 
    source_table,
    target_table,
    COUNT(*) as records_processed
FROM bronze.data_lineage 
WHERE DATE(created_at) = '2024-01-20'
GROUP BY source_table, target_table;
```

## 📈 PHASE 5: MONITORING & PERFORMANCE TESTING

### Grafana Dashboard Testing
1. Access Grafana: http://localhost:3000
2. Login with admin/admin
3. Import dashboard from: `/include/monitoring/metalayer_dashboard.json`
4. Verify metrics display:
   - Pipeline execution times
   - Data quality scores
   - Record processing rates
   - System resource usage

### Prometheus Metrics Testing
```powershell
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Query pipeline metrics
curl "http://localhost:9090/api/v1/query?query=metalayer_records_processed_total"
curl "http://localhost:9090/api/v1/query?query=metalayer_data_quality_score"
```

### Performance Benchmarks
```sql
-- Measure processing performance
SELECT 
    layer,
    AVG(processing_time) as avg_processing_seconds,
    AVG(records_processed/NULLIF(processing_time,0)) as records_per_second
FROM (
    -- Add your performance metrics query here
    SELECT 'bronze' as layer, 60 as processing_time, 10000 as records_processed
) AS performance_data
GROUP BY layer;
```

## 🔍 PHASE 6: ERROR HANDLING & RECOVERY TESTING

### Failure Scenario Testing

#### Test 1: Database Connection Failure
```powershell
# Simulate PostgreSQL failure
docker-compose stop postgres

# Trigger DAG and observe failure handling
docker-compose exec webserver airflow dags trigger bronze_layer_etl_pipeline

# Restart PostgreSQL and verify recovery
docker-compose start postgres
```

#### Test 2: Data Quality Failure
```sql
-- Insert invalid data to test quality checks
INSERT INTO bronze.customers_raw (customer_id, customer_name, email, source_system, validation_status)
VALUES ('INVALID001', '', 'not-an-email', 'TEST_SYSTEM', 'PENDING');

-- Trigger pipeline and verify quality validation catches issues
```

#### Test 3: Task Timeout Testing
```powershell
# Test task timeout by setting very low timeout
docker-compose exec webserver airflow variables set bronze_max_processing_minutes 1

# Trigger pipeline and verify timeout handling
docker-compose exec webserver airflow dags trigger bronze_layer_etl_pipeline
```

## ✅ SUCCESS CRITERIA CHECKLIST

### Deployment Validation
- [ ] All Docker services running and healthy
- [ ] Database schemas created successfully
- [ ] Airflow connections configured
- [ ] Airflow variables set correctly
- [ ] All DAGs imported without errors
- [ ] UI interfaces accessible

### Data Processing Validation  
- [ ] Bronze layer ingests source data successfully
- [ ] Silver layer transforms data with >95% quality score
- [ ] Gold layer creates dimensional models correctly
- [ ] End-to-end pipeline completes within SLA (4 hours)
- [ ] Data quality validations pass
- [ ] Business rules applied correctly

### Monitoring & Performance
- [ ] Grafana dashboards display metrics
- [ ] Prometheus collects pipeline metrics
- [ ] Performance meets benchmarks:
  - Bronze: < 60 minutes
  - Silver: < 120 minutes  
  - Gold: < 180 minutes
  - Total: < 240 minutes

### Error Handling
- [ ] Pipeline handles database failures gracefully
- [ ] Data quality issues trigger appropriate alerts
- [ ] Task timeouts work as expected
- [ ] Recovery procedures work correctly

## 🚨 TROUBLESHOOTING QUICK REFERENCE

### Common Issues & Solutions

#### DAG Import Errors
```powershell
# Check Python syntax
docker-compose exec webserver python -m py_compile /opt/airflow/dags/[dag_file].py

# Check Airflow logs
docker-compose logs webserver | grep ERROR
```

#### Database Connection Issues
```powershell
# Test database connectivity
docker-compose exec webserver airflow connections test postgres_default

# Check PostgreSQL logs
docker-compose logs postgres
```

#### Performance Issues
```powershell
# Check resource usage
docker stats

# Scale workers if needed
docker-compose up --scale worker=3 -d
```

#### Memory Issues
```powershell
# Check memory usage
docker stats --format "table {{.Container}}\t{{.MemUsage}}"

# Restart services if needed
docker-compose restart
```

## 📋 TESTING REPORT TEMPLATE

### Test Execution Summary
- **Test Date**: ___________
- **Environment**: Docker Development
- **Pipeline Version**: 1.0.0
- **Total Test Duration**: ___________

### Results
| Test Phase | Status | Duration | Notes |
|------------|--------|----------|--------|
| Deployment | ✅/❌ | ___ min | _____ |
| Configuration | ✅/❌ | ___ min | _____ |
| Bronze Layer | ✅/❌ | ___ min | _____ |
| Silver Layer | ✅/❌ | ___ min | _____ |
| Gold Layer | ✅/❌ | ___ min | _____ |
| End-to-End | ✅/❌ | ___ min | _____ |
| Monitoring | ✅/❌ | ___ min | _____ |
| Error Handling | ✅/❌ | ___ min | _____ |

### Performance Metrics
- **Records Processed**: _____________
- **Data Quality Score**: _____________%
- **Pipeline Throughput**: _________ records/sec
- **Resource Utilization**: CPU ___%, Memory ___%

### Issues Identified
1. ________________________________
2. ________________________________
3. ________________________________

### Recommendations
1. ________________________________
2. ________________________________
3. ________________________________

---

**MetaLayer ETL Pipeline v1.0.0** | Comprehensive Testing Guide