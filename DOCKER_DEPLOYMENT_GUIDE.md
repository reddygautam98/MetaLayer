# 🐳 Docker Deployment Guide
## Complete ETL Pipeline with Monitoring Stack

### 📋 Table of Contents
- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start) 
- [Service Configuration](#service-configuration)
- [Management Scripts](#management-scripts)
- [Monitoring & Alerting](#monitoring--alerting)
- [Performance Tuning](#performance-tuning)
- [Security Configuration](#security-configuration)
- [Backup & Recovery](#backup--recovery)
- [Troubleshooting](#troubleshooting)

---

## 🏗️ Architecture Overview

### Service Stack
```
┌─────────────────────────────────────────────────────────────────┐
│                    ETL Pipeline Docker Stack                    │
├─────────────────────────────────────────────────────────────────┤
│  🌐 Load Balancer/Proxy (Optional)                            │
├─────────────────────────────────────────────────────────────────┤
│  📊 Airflow Services                                           │
│  ├── Webserver (8080) - Web UI, API                          │
│  ├── Scheduler - DAG scheduling, task dispatch                │
│  ├── Worker(s) - Task execution with Celery                   │
│  └── Flower (5555) - Celery monitoring (Optional)             │
├─────────────────────────────────────────────────────────────────┤
│  💾 Data Layer                                                │
│  ├── PostgreSQL (5432) - Data Warehouse + Airflow DB         │
│  ├── Redis (6379) - Task Queue, Caching                      │
│  └── Persistent Volumes - logs, data, configs                 │
├─────────────────────────────────────────────────────────────────┤
│  📈 Monitoring Stack                                          │
│  ├── Prometheus (9090) - Metrics collection                   │
│  ├── Grafana (3000) - Dashboards, alerting                   │
│  ├── Node Exporter (9100) - System metrics                   │
│  └── ETL Metrics Exporter (9200) - Custom pipeline metrics    │
└─────────────────────────────────────────────────────────────────┘
```

### Network Architecture
- **Network**: `etl_pipeline_network` (172.20.0.0/16)
- **Service Discovery**: Container names + static IPs
- **Security**: Isolated network, no external access except defined ports

---

## 🚀 Quick Start

### Prerequisites
```powershell
# Required software
docker --version        # Docker 20.10+
docker compose version  # Docker Compose V2

# System requirements  
# RAM: 8GB minimum, 16GB recommended
# Disk: 10GB free space minimum
# CPU: 4+ cores recommended
```

### One-Command Deployment
```powershell
# Clone repository
git clone https://github.com/reddygautam98/MetaLayer.git
cd MetaLayer

# Start complete ETL stack
.\scripts\docker-deploy.ps1 -Action start

# Verify deployment
.\scripts\etl-monitor.ps1 -Operation health
```

### Service Access
After deployment, access these services:

| Service | URL | Default Credentials |
|---------|-----|-------------------|
| 🎯 **Airflow Web UI** | http://localhost:8080 | `admin` / `admin` |
| 📊 **Grafana** | http://localhost:3000 | `admin` / `admin_secure_2024` |
| 🔍 **Prometheus** | http://localhost:9090 | None |
| 📈 **ETL Metrics** | http://localhost:9200/metrics | None |

---

## ⚙️ Service Configuration

### Environment Variables (.env)
```ini
# =====================================================
# CORE DATABASE CONFIGURATION
# =====================================================
POSTGRES_USER=postgres
POSTGRES_PASSWORD=etl_secure_2024!
POSTGRES_DB=airflow
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# =====================================================
# AIRFLOW CONFIGURATION
# =====================================================
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__PARALLELISM=32
AIRFLOW__CORE__DAG_CONCURRENCY=16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=4

# Security Keys (Generate new ones for production!)
AIRFLOW_FERNET_KEY=Z2uDm8ZB7dM8N1pF9k3L4xQ2v8C6sE1nB7wR5tY3uH9
AIRFLOW_SECRET_KEY=etl_pipeline_secret_key_2024_docker

# =====================================================
# PORT CONFIGURATION  
# =====================================================
AIRFLOW_WEBSERVER_PORT=8080
GRAFANA_PORT=3000
PROMETHEUS_PORT=9090
POSTGRES_PORT=5432
REDIS_PORT=6379
```

### Resource Limits
```yaml
# Defined in docker-compose.yml
services:
  postgres:
    deploy:
      resources:
        limits:
          memory: 2g
          cpus: '1.0'
  
  airflow-webserver:
    deploy:
      resources:
        limits:
          memory: 2g
          cpus: '1.0'
  
  airflow-worker:
    deploy:
      resources:
        limits:
          memory: 3g
          cpus: '2.0'
```

---

## 🛠️ Management Scripts

### docker-deploy.ps1
**Primary deployment and management script**

```powershell
# Start services
.\scripts\docker-deploy.ps1 -Action start
.\scripts\docker-deploy.ps1 -Action start -Service postgres  # Start only PostgreSQL

# Stop services  
.\scripts\docker-deploy.ps1 -Action stop
.\scripts\docker-deploy.ps1 -Action stop -Service airflow-webserver

# Restart services
.\scripts\docker-deploy.ps1 -Action restart

# View status
.\scripts\docker-deploy.ps1 -Action status

# View logs
.\scripts\docker-deploy.ps1 -Action logs
.\scripts\docker-deploy.ps1 -Action logs -Service postgres

# Build images
.\scripts\docker-deploy.ps1 -Action build

# Complete cleanup
.\scripts\docker-deploy.ps1 -Action cleanup
```

### etl-monitor.ps1  
**Monitoring, debugging, and maintenance script**

```powershell
# Health checks
.\scripts\etl-monitor.ps1 -Operation health        # Comprehensive health check
.\scripts\etl-monitor.ps1 -Operation performance   # Performance metrics

# Logging and debugging
.\scripts\etl-monitor.ps1 -Operation logs          # View all service logs  
.\scripts\etl-monitor.ps1 -Operation debug         # Interactive debug session

# Backup and recovery
.\scripts\etl-monitor.ps1 -Operation backup        # Create full backup
.\scripts\etl-monitor.ps1 -Operation restore       # Restore from backup
```

---

## 📈 Monitoring & Alerting

### Prometheus Metrics
The system exposes comprehensive metrics:

**System Metrics (Node Exporter):**
- CPU usage, memory, disk I/O
- Network statistics
- File system usage

**Airflow Metrics (Custom Exporter):**
- DAG run success/failure rates  
- Task execution times
- Queue lengths and processing rates
- Database connection pool status

**ETL Pipeline Metrics:**
- Records processed per layer (Bronze/Silver/Gold)
- Data quality scores
- Processing throughput (records/second)
- Pipeline execution duration

### Grafana Dashboards

**📊 ETL Overview Dashboard:**
- Pipeline health status
- Real-time processing metrics
- Data quality trends
- System resource utilization

**🎯 Airflow Monitoring Dashboard:**  
- DAG success/failure rates
- Task duration trends
- Queue monitoring  
- Worker performance

**🖥️ System Monitoring Dashboard:**
- Container resource usage
- Database performance
- Network and disk I/O
- Memory and CPU trends

### Custom Metrics Examples
```python
# ETL pipeline custom metrics
metalayer_records_processed_total{layer="bronze",status="success"} 150000
metalayer_records_processed_total{layer="silver",status="success"} 149500  
metalayer_records_processed_total{layer="gold",status="success"} 149200

metalayer_data_quality_score{table="customers"} 0.98
metalayer_data_quality_score{table="orders"} 0.96

metalayer_pipeline_duration_seconds{dag_id="bronze_layer_production_load"} 180
metalayer_pipeline_duration_seconds{dag_id="silver_layer_production_transform"} 240
```

---

## ⚡ Performance Tuning

### Airflow Performance
```ini
# In .env file - Core performance settings
AIRFLOW__CORE__PARALLELISM=32                    # Max parallel tasks across all DAGs
AIRFLOW__CORE__DAG_CONCURRENCY=16                # Max tasks per DAG
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=4         # Max concurrent DAG runs
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16       # Max tasks per DAG run

# Celery worker settings
AIRFLOW__CELERY__WORKER_CONCURRENCY=8            # Tasks per worker
AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER=1    # Task prefetch limit

# Database connection pooling
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=10      # Connection pool size
AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=20   # Max overflow connections
```

### Database Performance
```sql
-- PostgreSQL optimizations (applied via config)
shared_buffers = 512MB
effective_cache_size = 1GB  
work_mem = 16MB
maintenance_work_mem = 256MB
max_connections = 100
```

### Resource Scaling
```yaml
# Scale workers horizontally in docker-compose.yml
airflow-worker:
  deploy:
    replicas: 3              # Run 3 worker containers
    resources:
      limits:
        memory: 2g
        cpus: '1.5'
```

---

## 🔒 Security Configuration

### Security Best Practices

**🔐 Secrets Management:**
```powershell
# Generate new Fernet key for production
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Update .env file with new keys
AIRFLOW_FERNET_KEY=your_generated_key_here
AIRFLOW_SECRET_KEY=your_secret_key_here
```

**🛡️ Network Security:**
- Services isolated in private network (172.20.0.0/16)
- Only necessary ports exposed to host
- No direct external access to database/Redis

**👤 Access Control:**
```python
# Airflow RBAC enabled by default
AIRFLOW__WEBSERVER__RBAC=True
AIRFLOW__WEBSERVER__AUTHENTICATE=True

# Create additional users via Web UI or CLI:
# Admin > Security > List Users > Add User
```

**🔍 Audit Logging:**
```ini
# Enhanced logging configuration
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
AIRFLOW__LOGGING__FAB_LOGGING_LEVEL=WARN

# All actions logged to:
# - Container logs: docker logs etl_airflow_webserver
# - Persistent logs: ./logs volume mount
```

---

## 💾 Backup & Recovery

### Automated Backup
```powershell
# Create comprehensive backup
.\scripts\etl-monitor.ps1 -Operation backup

# Backup includes:
# - PostgreSQL database dump (airflow_db.sql)
# - Airflow logs directory  
# - Configuration files (.env, docker-compose.yml)
# - Backup metadata (README.txt)
```

### Manual Backup Procedures
```powershell
# Database backup
docker exec etl_postgres pg_dump -U postgres -d airflow > backup_$(Get-Date -f "yyyyMMdd_HHmmss").sql

# Configuration backup
Copy-Item .env, docker-compose.yml backup_configs/

# Logs backup  
docker cp etl_airflow_webserver:/opt/airflow/logs ./backup_logs
```

### Disaster Recovery
```powershell
# Full system restore
.\scripts\etl-monitor.ps1 -Operation restore

# Manual restore process:
# 1. Stop all services
docker compose down -v

# 2. Start database only
docker compose up -d postgres

# 3. Restore database
Get-Content backup_airflow_db.sql | docker exec -i etl_postgres psql -U postgres -d airflow

# 4. Start all services
docker compose up -d
```

---

## 🐛 Troubleshooting

### Common Issues & Solutions

**🚫 Port Conflicts**
```powershell
# Problem: Port 8080 already in use
# Solution: Change port in .env
AIRFLOW_WEBSERVER_PORT=8081

# Or kill conflicting process
netstat -ano | findstr :8080
taskkill /PID <process_id> /F
```

**💾 Database Issues**
```powershell
# Problem: Database connection failures
# Diagnosis:
docker exec etl_postgres pg_isready -U postgres

# Solution: Reset database
docker compose down postgres
docker volume rm metalayer_postgres_data
docker compose up -d postgres
```

**🔄 Service Startup Failures**
```powershell  
# Problem: Services exit immediately
# Diagnosis:
.\scripts\etl-monitor.ps1 -Operation health
docker compose logs <service_name>

# Common solutions:
# 1. Check resource availability
docker system df
docker system prune -f

# 2. Restart with fresh containers
docker compose down
docker compose up -d --force-recreate
```

**📊 DAG Issues**
```powershell
# Problem: DAGs not appearing in UI
# Diagnosis:
docker exec etl_airflow_webserver airflow dags list-import-errors

# Solutions:
# 1. Check DAG file syntax
# 2. Restart scheduler
docker compose restart airflow-scheduler

# 3. Refresh DAG folder
docker exec etl_airflow_webserver airflow dags reserialize
```

### Emergency Procedures

**🆘 Complete System Reset**
```powershell
# WARNING: This destroys all data!
# Use only as last resort

# 1. Stop everything
docker compose down -v --remove-orphans

# 2. Clean Docker resources
docker system prune -af
docker volume prune -f

# 3. Remove project volumes
docker volume ls | findstr metalayer | ForEach-Object { docker volume rm $_.Split()[-1] }

# 4. Fresh start
.\scripts\docker-deploy.ps1 -Action start
```

**🔧 Service-Specific Resets**
```powershell
# Reset only Airflow (preserve data)
docker compose stop airflow-webserver airflow-scheduler airflow-worker
docker compose rm airflow-webserver airflow-scheduler airflow-worker
docker compose up -d airflow-webserver airflow-scheduler airflow-worker

# Reset only monitoring
docker compose stop prometheus grafana
docker volume rm metalayer_prometheus_data metalayer_grafana_data
docker compose up -d prometheus grafana
```

---

## 📞 Support & Maintenance

### Regular Maintenance Tasks

**📋 Weekly Tasks:**
```powershell
# System health check
.\scripts\etl-monitor.ps1 -Operation health

# Performance review
.\scripts\etl-monitor.ps1 -Operation performance

# Backup creation
.\scripts\etl-monitor.ps1 -Operation backup

# Clean old Docker resources
docker system prune -f
```

**🗑️ Monthly Tasks:**
```powershell
# Update Docker images
docker compose pull
docker compose up -d

# Archive old logs
docker exec etl_airflow_webserver find /opt/airflow/logs -mtime +30 -delete

# Database maintenance
docker exec etl_postgres psql -U postgres -d airflow -c "VACUUM ANALYZE;"
```

### Getting Help

**📚 Resources:**
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Prometheus Monitoring](https://prometheus.io/docs/)
- [Grafana Dashboard Creation](https://grafana.com/docs/)

**🐛 Issue Reporting:**
When reporting issues, include:
```powershell
# System information
docker version
docker compose version
.\scripts\etl-monitor.ps1 -Operation health

# Relevant logs
docker compose logs > system_logs.txt

# Configuration (remove sensitive data)
# .env file contents
# docker-compose.yml modifications
```

---

*This Docker deployment provides a production-ready ETL pipeline with comprehensive monitoring, automated backups, and enterprise-grade security. Perfect for data engineering teams requiring scalable, maintainable data processing infrastructure.*