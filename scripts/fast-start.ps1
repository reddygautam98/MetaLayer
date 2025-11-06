#!/usr/bin/env pwsh
# =====================================================
# FAST ETL STARTUP SCRIPT
# Optimized Docker Compose startup with health checks
# =====================================================

param(
    [string]$Mode = "full",  # full, minimal, services-only
    [switch]$SkipBuild = $false,
    [switch]$CleanStart = $false
)

Write-Host "🚀 MetaLayer ETL Pipeline - Fast Startup" -ForegroundColor Green
Write-Host "=" * 50

# Change to project directory
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectDir

# Clean start option
if ($CleanStart) {
    Write-Host "🧹 Cleaning up existing containers and volumes..." -ForegroundColor Yellow
    docker-compose down --volumes --remove-orphans
    docker system prune -f
}

# Build optimization
$BuildArgs = @()
if ($SkipBuild) {
    $BuildArgs += "--no-build"
    Write-Host "⏩ Skipping image build..." -ForegroundColor Yellow
} else {
    Write-Host "🔨 Building optimized images..." -ForegroundColor Blue
    # Pre-pull base images to speed up build
    docker pull apache/airflow:2.8.1-python3.11 2>$null
    docker pull postgres:15 2>$null
    docker pull redis:7-alpine 2>$null
}

# Service startup based on mode
switch ($Mode) {
    "minimal" {
        Write-Host "🎯 Starting minimal services (PostgreSQL + Redis + Airflow)..." -ForegroundColor Blue
        $Services = @("postgres", "redis", "airflow-init", "webserver", "scheduler")
        docker-compose up -d @BuildArgs $Services
    }
    "services-only" {
        Write-Host "🎯 Starting core services only (no monitoring)..." -ForegroundColor Blue
        $Services = @("postgres", "redis", "airflow-init", "webserver", "scheduler", "worker")
        docker-compose up -d @BuildArgs $Services
    }
    default {
        Write-Host "🎯 Starting full ETL stack..." -ForegroundColor Blue
        docker-compose up -d @BuildArgs
    }
}

# Wait for services to be ready
Write-Host "⏳ Waiting for services to start..." -ForegroundColor Yellow

# Check PostgreSQL first (critical dependency)
$MaxRetries = 30
$RetryCount = 0
do {
    $PostgresStatus = docker-compose exec postgres pg_isready -U airflow -q 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ PostgreSQL is ready" -ForegroundColor Green
        break
    }
    Start-Sleep 2
    $RetryCount++
    Write-Host "." -NoNewline
} while ($RetryCount -lt $MaxRetries)

if ($RetryCount -eq $MaxRetries) {
    Write-Host "❌ PostgreSQL failed to start within timeout" -ForegroundColor Red
    exit 1
}

# Check Redis
$RetryCount = 0
do {
    $RedisStatus = docker-compose exec redis redis-cli ping 2>$null
    if ($RedisStatus -eq "PONG") {
        Write-Host "✅ Redis is ready" -ForegroundColor Green
        break
    }
    Start-Sleep 1
    $RetryCount++
} while ($RetryCount -lt 15)

# Check Airflow Webserver
$RetryCount = 0
do {
    try {
        $WebStatus = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 5 -UseBasicParsing 2>$null
        if ($WebStatus.StatusCode -eq 200) {
            Write-Host "✅ Airflow Webserver is ready" -ForegroundColor Green
            break
        }
    } catch {
        # Continue retrying
    }
    Start-Sleep 3
    $RetryCount++
    Write-Host "." -NoNewline
} while ($RetryCount -lt 20)

# Initialize database schema if needed
Write-Host "🗄️ Initializing database schema..." -ForegroundColor Blue
$SchemaCheck = docker-compose exec postgres psql -U airflow -d airflow -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'bronze' AND table_name = 'customers_raw');" 2>$null

if ($SchemaCheck -notlike "*t*") {
    Write-Host "📊 Creating database schemas..." -ForegroundColor Yellow
    docker-compose exec postgres psql -U airflow -d airflow -f /opt/airflow/include/sql/complete_schema_docker.sql >$null 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Database schema created successfully" -ForegroundColor Green
    } else {
        Write-Host "⚠️ Schema creation had issues (may already exist)" -ForegroundColor Yellow
    }
} else {
    Write-Host "✅ Database schema already exists" -ForegroundColor Green
}

# Setup Airflow connections and variables
Write-Host "🔧 Configuring Airflow..." -ForegroundColor Blue
$ConfigExists = docker-compose exec webserver airflow connections list --output table 2>$null | Select-String "postgres_default"

if (-not $ConfigExists) {
    Write-Host "⚙️ Setting up Airflow connections and variables..." -ForegroundColor Yellow
    
    # Key connections setup
    docker-compose exec webserver airflow connections add postgres_default --conn-type postgres --conn-host postgres --conn-login airflow --conn-password airflow_password --conn-schema airflow --conn-port 5432 >$null 2>&1
    docker-compose exec webserver airflow connections add redis_default --conn-type redis --conn-host redis --conn-port 6379 >$null 2>&1
    
    # Essential variables
    docker-compose exec webserver airflow variables set pipeline_version "1.0.0" >$null 2>&1
    docker-compose exec webserver airflow variables set data_quality_threshold "0.95" >$null 2>&1
    docker-compose exec webserver airflow variables set bronze_batch_size "10000" >$null 2>&1
    
    Write-Host "✅ Airflow configuration completed" -ForegroundColor Green
} else {
    Write-Host "✅ Airflow already configured" -ForegroundColor Green
}

# Final status check
Write-Host "`n🎯 Service Status:" -ForegroundColor Blue
docker-compose ps

Write-Host "`n🌐 Access Points:" -ForegroundColor Green
Write-Host "   Airflow UI:    http://localhost:8080 (admin/admin)" -ForegroundColor Cyan
Write-Host "   Grafana:       http://localhost:3000 (admin/admin)" -ForegroundColor Cyan  
Write-Host "   Prometheus:    http://localhost:9090" -ForegroundColor Cyan
Write-Host "   Flower:        http://localhost:5555" -ForegroundColor Cyan

# Quick DAG status check
Write-Host "`n📊 DAG Status:" -ForegroundColor Blue
try {
    $DagList = docker-compose exec webserver airflow dags list --output table 2>$null
    if ($DagList) {
        Write-Host "✅ DAGs are loaded and ready" -ForegroundColor Green
        Write-Host "🚀 Ready to start ETL pipeline!" -ForegroundColor Green
        Write-Host "`n💡 Quick Start Commands:" -ForegroundColor Yellow
        Write-Host "   Start Full Pipeline: docker-compose exec webserver airflow dags trigger master_etl_orchestrator" -ForegroundColor White
        Write-Host "   Monitor Progress:    docker-compose logs -f scheduler" -ForegroundColor White
    } else {
        Write-Host "⚠️ DAGs not yet loaded (may take a few more minutes)" -ForegroundColor Yellow
    }
} catch {
    Write-Host "⚠️ Airflow not fully ready yet" -ForegroundColor Yellow
}

Write-Host "`n🎉 ETL Pipeline startup completed!" -ForegroundColor Green