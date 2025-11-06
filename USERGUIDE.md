# 📖 MetaLayer User Guide

*Complete guide to using the MetaLayer Data Pipeline Platform*

## 📋 Table of Contents

1. [Getting Started](#-getting-started)
2. [Platform Overview](#-platform-overview)
3. [Using the Airflow UI](#-using-the-airflow-ui)
4. [Monitoring with Grafana](#-monitoring-with-grafana)
5. [Data Pipeline Operations](#-data-pipeline-operations)
6. [Data Quality Management](#-data-quality-management)
7. [Troubleshooting Guide](#-troubleshooting-guide)
8. [Advanced Usage](#-advanced-usage)
9. [Best Practices](#-best-practices)
10. [FAQ](#-faq)

---

## 🚀 Getting Started

### Step 1: First Time Setup

1. **Verify Prerequisites**
   ```bash
   # Check Docker installation
   docker --version
   docker-compose --version
   
   # Verify minimum system requirements
   # - 8GB+ RAM recommended
   # - 20GB+ available disk space
   ```

2. **Clone and Start MetaLayer**
   ```bash
   git clone https://github.com/reddygautam98/MetaLayer.git
   cd MetaLayer
   
   # Start all services
   docker-compose up -d
   
   # Wait for services to be ready (2-3 minutes)
   docker ps --format "table {{.Names}}\t{{.Status}}"
   ```

3. **Verify Installation**
   ```bash
   # All containers should show "healthy" status
   # Expected containers:
   # - metalayer-webserver-1 (Airflow UI)
   # - metalayer-scheduler-1 (Airflow Scheduler) 
   # - metalayer-triggerer-1 (Airflow Triggerer)
   # - metalayer_postgres (Database)
   # - metalayer_grafana (Monitoring)
   # - metalayer_prometheus (Metrics)
   # - metalayer_node_exporter (System metrics)
   ```

### Step 2: Access Platform Services

| Service | URL | Login | Purpose |
|---------|-----|-------|---------|
| **Airflow UI** | http://localhost:8081 | admin/admin | DAG management & monitoring |
| **Grafana** | http://localhost:3000 | admin/admin | Dashboards & visualization |
| **Prometheus** | http://localhost:9090 | - | Metrics & alerting |

---

## 🏗️ Platform Overview

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        MetaLayer Platform                        │
├─────────────────────────────────────────────────────────────────┤
│  Data Sources → Bronze → Silver → Gold → Analytics              │
│                    ↓       ↓       ↓                           │
│               Data Quality Monitoring                            │
│                    ↓                                           │
│         Grafana Dashboards ← Prometheus Metrics                 │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Stages

1. **🥉 Bronze Layer (Raw Data)**
   - **Purpose**: Store raw, unprocessed data from source systems
   - **Tables**: `bronze.erp_sales_raw`, `bronze.crm_customers_raw`, `bronze.erp_orders_raw`
   - **Characteristics**: 
     - Exact copy of source data
     - Minimal data types (mostly text)
     - No business rules applied
     - Audit trails included (loaded_timestamp)

2. **🥈 Silver Layer (Cleaned Data)**
   - **Purpose**: Cleaned, validated, and standardized data
   - **Tables**: `silver.sales_cleaned`, `silver.customers_standardized`, `silver.orders`
   - **Characteristics**:
     - Data type enforcement
     - Quality validations applied
     - Deduplication performed
     - Standardized formats (dates, emails, phone numbers)

3. **🥇 Gold Layer (Analytics Ready)**
   - **Purpose**: Business-ready dimensional models for reporting
   - **Tables**: `gold.sales_summary`, `gold.customer_analytics`, `gold.dim_customer`, `gold.fact_sales`
   - **Characteristics**:
     - Aggregated metrics
     - Fact/dimension model structure
     - Optimized for analytical queries
     - Business logic applied

---

## 🎛️ Using the Airflow UI

### Accessing the DAG Interface

1. **Navigate to Airflow**: http://localhost:8081
2. **Login**: Username `admin`, Password `admin`
3. **Main Dashboard**: Shows all available DAGs

### Understanding DAG Status

| Status | Icon | Meaning |
|--------|------|---------|
| **Success** | 🟢 Green Circle | DAG run completed successfully |
| **Running** | 🔵 Blue Circle | DAG is currently executing |
| **Failed** | 🔴 Red Circle | DAG run encountered errors |
| **Queued** | 🟡 Yellow Circle | DAG is waiting to execute |
| **Paused** | ⏸️ Pause Icon | DAG is disabled |

### Key Production DAGs

#### 1. `bronze_layer_production_load`
- **Purpose**: Load raw data from source files into bronze tables
- **Trigger**: Manual or API call
- **Duration**: ~3-5 minutes for 1M records
- **Monitor**: Check task `load_erp_sales_with_validation` for progress

#### 2. `silver_layer_production_transform` 
- **Purpose**: Transform bronze data into cleaned silver tables
- **Trigger**: After bronze layer completion
- **Duration**: ~2-3 minutes
- **Monitor**: Watch `clean_sales_data` and `standardize_customers` tasks

#### 3. `gold_layer_production_analytics`
- **Purpose**: Create analytics-ready dimensional models
- **Trigger**: After silver layer completion  
- **Duration**: ~1-2 minutes
- **Monitor**: Check `build_customer_analytics` and `create_sales_summary` tasks

#### 4. `data_quality_monitoring`
- **Purpose**: Validate data quality across all layers
- **Trigger**: Every 6 hours (automatic)
- **Duration**: ~30 seconds
- **Monitor**: Review quality metrics in task logs

### Common DAG Operations

#### Manually Triggering a DAG
1. Click the DAG name to open details
2. Click the "Play" button (▶️) in the top-right corner
3. Optionally add configuration JSON
4. Click "Trigger DAG"

#### Viewing Task Details
1. Click on a DAG run (colored circle)
2. Select "Graph" view to see task dependency
3. Click individual tasks to see logs and details
4. Use "Gantt" view to analyze execution timing

#### Monitoring DAG Progress
1. **Grid View**: Shows historical runs and task status
2. **Graph View**: Displays task dependencies and current status
3. **Calendar View**: Shows success/failure patterns over time
4. **Duration View**: Analyzes execution time trends

---

## 📊 Monitoring with Grafana

### Accessing Dashboards

1. **Navigate to Grafana**: http://localhost:3000
2. **Login**: Username `admin`, Password `admin`  
3. **Dashboards**: Click "Dashboards" in left menu

### Available Dashboards

#### 1. MetaLayer Overview Dashboard
- **Pipeline Health**: Overall system status
- **Records Processed**: Count by layer (Bronze/Silver/Gold)
- **Data Freshness**: Time since last successful update
- **Error Rates**: Failure percentage by component

#### 2. Airflow Monitoring Dashboard
- **DAG Success Rate**: Percentage of successful runs
- **Task Execution Times**: Performance metrics
- **Resource Utilization**: CPU, memory, disk usage
- **Queue Status**: Pending task counts

#### 3. Data Quality Dashboard
- **Quality Scores**: Data quality metrics (0-1 scale)
- **Validation Results**: Pass/fail counts by check type
- **Anomaly Detection**: Unusual data patterns
- **Data Profiling**: Statistical summaries

### Interpreting Metrics

#### Key Performance Indicators (KPIs)
- **Pipeline Success Rate**: Should be >95%
- **Data Processing Latency**: <5 minutes for full pipeline
- **Data Quality Score**: Should be >0.9 (90%+)
- **System Uptime**: Should be >99.9%

#### Alert Thresholds
- 🟢 **Green**: All metrics within normal ranges
- 🟡 **Yellow**: Performance degradation detected
- 🔴 **Red**: Critical issues requiring attention

### Setting Up Alerts

1. **Navigate to Alerting** → **Alert Rules**
2. **Create New Rule** with conditions like:
   - Pipeline failure rate >5%
   - Data processing time >10 minutes  
   - Data quality score <0.8
3. **Configure Notifications** (Email, Slack, etc.)

---

## ⚙️ Data Pipeline Operations

### Running the Complete Pipeline

#### Option 1: Automatic Sequence (Recommended)
```bash
# Trigger bronze layer (others will follow automatically)
curl -X POST "http://localhost:8081/api/v1/dags/bronze_layer_production_load/dagRuns" \
     -H "Content-Type: application/json" \
     -u admin:admin \
     -d '{}'
```

#### Option 2: Manual Step-by-Step
1. **Start Bronze Layer**
   - Go to `bronze_layer_production_load` DAG
   - Click "Trigger DAG"
   - Wait for completion (~3-5 minutes)

2. **Run Silver Transformation**
   - Go to `silver_layer_production_transform` DAG  
   - Click "Trigger DAG"
   - Wait for completion (~2-3 minutes)

3. **Execute Gold Analytics**
   - Go to `gold_layer_production_analytics` DAG
   - Click "Trigger DAG" 
   - Wait for completion (~1-2 minutes)

4. **Verify Data Quality**
   - Go to `data_quality_monitoring` DAG
   - Click "Trigger DAG"
   - Review quality metrics

### Verifying Pipeline Success

#### Check Data Counts
```bash
# Connect to database
docker exec -it metalayer_postgres psql -U postgres -d airflow

# Verify record counts
SELECT 
  'Bronze' as layer, COUNT(*) as records 
FROM bronze.erp_sales_raw
UNION ALL
SELECT 
  'Silver', COUNT(*) 
FROM silver.sales_cleaned  
UNION ALL
SELECT 
  'Gold', COUNT(*) 
FROM gold.sales_summary;
```

#### Expected Results
- **Bronze**: 1,000,000 records
- **Silver**: 1,000,000 records (after cleaning)
- **Gold**: 639 summary records (aggregated)

### Managing Data Refresh

#### Full Refresh (Monthly)
```bash
# Clear all data and reload completely
docker exec metalayer-webserver-1 airflow dags trigger full_pipeline_refresh
```

#### Incremental Updates (Daily)
```bash
# Process only new/changed data
docker exec metalayer-webserver-1 airflow dags trigger incremental_pipeline_update
```

---

## 🔍 Data Quality Management

### Understanding Quality Checks

#### Bronze Layer Validations
- **File Integrity**: Verify source file completeness
- **Schema Validation**: Confirm expected columns exist
- **Record Count**: Ensure minimum data volume
- **Duplicate Detection**: Identify potential duplicate records

#### Silver Layer Validations  
- **Data Type Compliance**: Verify proper type casting
- **Business Rule Validation**: Check domain-specific constraints
- **Referential Integrity**: Validate foreign key relationships
- **Statistical Analysis**: Detect anomalies in data distributions

#### Gold Layer Validations
- **Aggregation Accuracy**: Verify calculated metrics
- **Dimensional Model Integrity**: Check fact/dimension consistency  
- **Business Logic Validation**: Confirm analytical rule application
- **Historical Consistency**: Compare with previous periods

### Quality Monitoring Workflow

1. **Automated Checks**: Run every 6 hours via `data_quality_monitoring` DAG
2. **Quality Scoring**: Each check receives 0-1 score (1=perfect quality)
3. **Alert Generation**: Scores <0.8 trigger notifications  
4. **Issue Investigation**: Failed checks logged with details
5. **Remediation**: Automatic or manual correction procedures

### Investigating Quality Issues

#### Step 1: Check Grafana Dashboard
1. Open "Data Quality Dashboard" 
2. Identify failing quality checks
3. Note affected tables and time ranges

#### Step 2: Review DAG Logs
1. Go to `data_quality_monitoring` DAG in Airflow
2. Click on failed task
3. Review detailed logs for specific error messages

#### Step 3: Database Investigation
```sql
-- Check quality metrics table
SELECT * FROM airflow_meta.data_quality_results 
WHERE quality_score < 0.8 
ORDER BY check_timestamp DESC;

-- Examine specific data issues
SELECT * FROM silver.sales_cleaned 
WHERE [identified_problem_field] IS NULL;
```

### Quality Improvement Actions

#### Common Issues & Solutions

1. **Missing Data**
   - **Issue**: NULL values in required fields
   - **Solution**: Update source systems or implement default values

2. **Data Type Mismatches**
   - **Issue**: Text in numeric fields  
   - **Solution**: Add data cleansing rules in silver layer

3. **Referential Integrity Violations**
   - **Issue**: Orphaned records without valid references
   - **Solution**: Implement lookup tables or exclusion rules

4. **Statistical Anomalies**
   - **Issue**: Values outside expected ranges
   - **Solution**: Add business rule validations or investigate source changes

---

## 🛠️ Troubleshooting Guide

### Common Issues & Solutions

#### 1. DAG Import Errors

**Symptoms**: DAG doesn't appear in Airflow UI

**Diagnosis**:
```bash
# Check for import errors
docker exec metalayer-webserver-1 airflow dags list-import-errors
```

**Solutions**:
- Fix Python syntax errors in DAG files
- Ensure all required dependencies are installed
- Check file permissions in `/opt/airflow/dags/`

#### 2. Database Connection Issues  

**Symptoms**: Tasks fail with connection errors

**Diagnosis**:
```bash
# Test database connection
docker exec metalayer-webserver-1 airflow connections test postgres_default
```

**Solutions**:
- Verify PostgreSQL container is running
- Check connection settings in Airflow Variables
- Restart database container if needed:
  ```bash
  docker restart metalayer_postgres
  ```

#### 3. Memory/Performance Issues

**Symptoms**: Slow DAG execution, container restarts

**Diagnosis**:
```bash
# Check resource usage
docker stats metalayer-webserver-1 metalayer-scheduler-1

# Review container logs
docker logs metalayer-webserver-1 --tail 100
```

**Solutions**:
- Increase Docker memory allocation (8GB+ recommended)
- Reduce DAG parallelism in `airflow.cfg`
- Optimize SQL queries for large datasets

#### 4. Data Loading Failures

**Symptoms**: Bronze layer DAGs fail during CSV loading

**Diagnosis**:
- Check file paths in `/opt/airflow/data/bronze_src/`
- Verify CSV file format and encoding
- Review task logs for specific error messages

**Solutions**:
- Ensure CSV files match expected schema
- Check file encoding (UTF-8 recommended)
- Verify file permissions and accessibility

### Log Analysis

#### Airflow Logs
```bash
# Web server logs
docker logs metalayer-webserver-1 -f

# Scheduler logs  
docker logs metalayer-scheduler-1 -f

# Task-specific logs (from Airflow UI)
# Navigate to DAG → Task → Logs tab
```

#### Database Logs
```bash
# PostgreSQL logs
docker logs metalayer_postgres -f

# Query performance
docker exec metalayer_postgres psql -U postgres -d airflow -c "
SELECT query, calls, total_time, mean_time 
FROM pg_stat_statements 
ORDER BY total_time DESC LIMIT 10;"
```

#### System Monitoring
```bash
# Container resource usage
docker stats --no-stream

# Disk usage
docker system df

# Network connectivity
docker exec metalayer-webserver-1 ping metalayer_postgres
```

### Recovery Procedures

#### Complete System Restart
```bash
# Stop all services
docker-compose down

# Clean up resources (optional)
docker system prune -f

# Restart with fresh state
docker-compose up -d

# Wait for services to be ready
sleep 60

# Verify system health
docker ps --format "table {{.Names}}\t{{.Status}}"
```

#### Database Recovery
```bash
# Backup current state
docker exec metalayer_postgres pg_dump -U postgres -d airflow > backup.sql

# Reset database (if needed)
docker-compose down
docker volume rm metalayer_postgres_data
docker-compose up -d

# Restore from backup
docker exec -i metalayer_postgres psql -U postgres -d airflow < backup.sql
```

---

## 🚀 Advanced Usage

### Custom DAG Development

#### Creating a New DAG
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'custom_data_pipeline',
    default_args=default_args,
    description='Custom data processing pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['custom', 'production']
)

def custom_processing_function(**context):
    # Your custom logic here
    print("Processing custom data...")
    return "Success"

process_data = PythonOperator(
    task_id='process_custom_data',
    python_callable=custom_processing_function,
    dag=dag
)
```

#### Adding SQL Templates
1. Create SQL file in `include/sql/custom_transform.sql`
2. Reference in DAG using `SQLExecuteQueryOperator`
3. Use Jinja templating for dynamic values

### API Integration

#### Triggering DAGs via REST API
```bash
# Trigger with configuration
curl -X POST "http://localhost:8081/api/v1/dags/bronze_layer_production_load/dagRuns" \
     -H "Content-Type: application/json" \
     -u admin:admin \
     -d '{
       "conf": {
         "source_path": "/custom/data/path",
         "processing_date": "2025-11-05"
       }
     }'

# Get DAG status
curl "http://localhost:8081/api/v1/dags/bronze_layer_production_load" \
     -u admin:admin

# List recent DAG runs
curl "http://localhost:8081/api/v1/dags/bronze_layer_production_load/dagRuns?limit=10" \
     -u admin:admin
```

#### Monitoring via API
```bash
# Get task instance status
curl "http://localhost:8081/api/v1/dags/bronze_layer_production_load/dagRuns/manual__2025-11-05T12:00:00+00:00/taskInstances" \
     -u admin:admin

# Export DAG structure
curl "http://localhost:8081/api/v1/dags/bronze_layer_production_load/details" \
     -u admin:admin
```

### Performance Optimization

#### Database Tuning
```sql
-- Create indexes for frequently queried columns
CREATE INDEX CONCURRENTLY idx_sales_date ON silver.sales_cleaned(sale_date);
CREATE INDEX CONCURRENTLY idx_customer_id ON silver.sales_cleaned(customer_id);

-- Partition large tables by date
CREATE TABLE silver.sales_partitioned (
    LIKE silver.sales_cleaned INCLUDING ALL
) PARTITION BY RANGE (sale_date);

-- Analyze table statistics
ANALYZE silver.sales_cleaned;
```

#### Airflow Configuration
```python
# airflow.cfg optimizations
[core]
parallelism = 16
max_active_runs_per_dag = 4
max_active_tasks_per_dag = 8

[celery]  
worker_concurrency = 8

[scheduler]
dag_dir_list_interval = 300
parsing_processes = 4
```

### Data Lineage Tracking

#### Implementing Lineage Metadata
```python
from airflow.lineage.entities import File, Table

# Define data lineage
source_file = File("/opt/airflow/data/sales.csv")
bronze_table = Table("bronze.erp_sales_raw")
silver_table = Table("silver.sales_cleaned")

# Add to DAG tasks
load_task.add_inlets([source_file])
load_task.add_outlets([bronze_table])

transform_task.add_inlets([bronze_table])
transform_task.add_outlets([silver_table])
```

---

## ✅ Best Practices

### DAG Design Principles

1. **Idempotency**: DAGs should produce same results when re-run
2. **Atomicity**: Each task should be a single, indivisible operation
3. **Error Handling**: Implement retry logic and failure notifications
4. **Documentation**: Include clear descriptions and comments
5. **Testing**: Validate DAGs before production deployment

### Data Management

1. **Schema Evolution**: Plan for adding/modifying columns over time
2. **Data Retention**: Implement archival policies for old data
3. **Security**: Use appropriate access controls and data masking
4. **Backup Strategy**: Regular backups of critical data and configurations
5. **Monitoring**: Continuous observation of data quality and pipeline health

### Performance Guidelines

1. **Chunked Processing**: Break large datasets into manageable chunks
2. **Parallel Execution**: Use task groups for independent operations
3. **Resource Management**: Set appropriate memory and CPU limits
4. **Caching**: Reuse computed results where possible
5. **Indexing**: Create database indexes for frequently accessed columns

### Security Considerations

1. **Access Control**: Implement role-based permissions
2. **Secrets Management**: Use Airflow Variables/Connections for sensitive data
3. **Network Security**: Restrict container communication
4. **Audit Logging**: Track all data access and modifications
5. **Data Encryption**: Encrypt sensitive data at rest and in transit

---

## ❓ FAQ

### General Questions

**Q: How much data can MetaLayer process?**
A: MetaLayer is designed to handle 1M+ records efficiently. Performance scales with available system resources (RAM, CPU, storage).

**Q: Can I add custom data sources?**
A: Yes! Create new DAGs following the medallion architecture pattern. Add source-specific operators in the bronze layer.

**Q: How do I schedule automatic pipeline execution?**
A: Edit the DAG's `schedule_interval` parameter. Use cron expressions or Airflow's built-in schedules (`@daily`, `@hourly`, etc.).

**Q: Is MetaLayer suitable for production use?**
A: Yes! MetaLayer includes production-ready features like monitoring, error handling, data quality checks, and CI/CD automation.

### Technical Questions  

**Q: How do I scale MetaLayer for larger datasets?**
A: 
- Increase Docker memory allocation
- Adjust Airflow parallelism settings
- Implement data partitioning
- Consider distributed processing with Celery workers

**Q: Can I integrate with external data sources?**
A: Yes! MetaLayer supports various connectors:
- Database connections (MySQL, SQL Server, Oracle)
- API integrations (REST, GraphQL)
- Cloud storage (AWS S3, Azure Blob, GCS)
- Message queues (Kafka, RabbitMQ)

**Q: How do I backup MetaLayer data?**
A: 
```bash
# Database backup
docker exec metalayer_postgres pg_dump -U postgres -d airflow > metalayer_backup.sql

# Configuration backup  
docker cp metalayer-webserver-1:/opt/airflow/airflow.cfg ./airflow_config_backup.cfg

# DAG backup
cp -r dags/ dags_backup/
```

**Q: How do I migrate MetaLayer to a new environment?**
A: 
1. Export configurations and DAGs
2. Backup database using pg_dump
3. Deploy MetaLayer on new environment
4. Restore database and configurations
5. Verify all services are operational

### Troubleshooting Questions

**Q: Why is my DAG not appearing in Airflow UI?**
A: Check for:
- Python syntax errors in DAG files
- Missing dependencies in requirements.txt
- File permissions issues
- DAG import errors (check `airflow dags list-import-errors`)

**Q: Why are my tasks failing with database errors?**
A: Common causes:
- Database connection issues
- Table/schema doesn't exist  
- Insufficient database permissions
- Resource constraints (memory/CPU)

**Q: How do I improve slow pipeline performance?**
A: 
- Monitor resource usage (`docker stats`)
- Optimize SQL queries
- Increase parallelism settings
- Consider data partitioning
- Add database indexes

**Q: What should I do if containers keep restarting?**
A: 
- Check container logs (`docker logs [container_name]`)
- Verify system resources (8GB+ RAM recommended)
- Review Docker memory allocation
- Check for port conflicts

---

## 📞 Getting Help

### Documentation Resources
- **README.md**: Quick start and overview
- **This User Guide**: Comprehensive usage instructions
- **API Documentation**: REST endpoint references  
- **Code Comments**: Inline documentation in DAG files

### Support Channels  
- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: Community questions and help
- **Email Support**: support@metalayer.io
- **Documentation Site**: https://metalayer.readthedocs.io

### Community
- **Slack Channel**: #metalayer-users
- **Monthly Webinars**: Best practices and new features
- **User Forum**: Share experiences and solutions

---

*This user guide is updated regularly. Check the latest version at: https://github.com/reddygautam98/MetaLayer/blob/main/USERGUIDE.md*

**Built with ❤️ by the MetaLayer Team**