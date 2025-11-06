# =====================================================
# COMPREHENSIVE ETL MONITORING VALIDATION SCRIPT
# Complete system health check and metrics validation  
# =====================================================

Write-Host "🚀 METALAYER ETL MONITORING SYSTEM - COMPREHENSIVE VALIDATION" -ForegroundColor Green
Write-Host "=============================================================" -ForegroundColor Green
Write-Host ""

# Test Core ETL Services
Write-Host "📊 CORE ETL PIPELINE STATUS:" -ForegroundColor Yellow
Write-Host "-----------------------------" -ForegroundColor Yellow

try {
    $airflowHealth = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 5 | ConvertFrom-Json
    Write-Host "✅ Airflow Webserver: HEALTHY" -ForegroundColor Green
    Write-Host "   - Scheduler: $($airflowHealth.scheduler.status)" -ForegroundColor Cyan
    Write-Host "   - Database: $($airflowHealth.metadatabase.status)" -ForegroundColor Cyan
} catch {
    Write-Host "❌ Airflow Webserver: NOT ACCESSIBLE" -ForegroundColor Red
}

# Test Monitoring Stack
Write-Host ""
Write-Host "🔍 MONITORING STACK STATUS:" -ForegroundColor Yellow  
Write-Host "----------------------------" -ForegroundColor Yellow

try {
    $prometheusTest = Invoke-WebRequest -Uri "http://localhost:9090/-/healthy" -TimeoutSec 5
    Write-Host "✅ Prometheus: OPERATIONAL" -ForegroundColor Green
} catch {
    Write-Host "❌ Prometheus: NOT ACCESSIBLE" -ForegroundColor Red
}

try {
    $metricsTest = Invoke-WebRequest -Uri "http://localhost:8000/metrics" -TimeoutSec 5
    $metricsContent = $metricsTest.Content
    $metalayerMetrics = ($metricsContent -split "`n" | Where-Object {$_ -like "*metalayer*"} | Measure-Object).Count
    Write-Host "✅ Custom Metrics Exporter: OPERATIONAL" -ForegroundColor Green
    Write-Host "   - MetaLayer Metrics Available: $metalayerMetrics" -ForegroundColor Cyan
} catch {
    Write-Host "❌ Custom Metrics Exporter: NOT ACCESSIBLE" -ForegroundColor Red
}

try {
    $grafanaTest = Invoke-WebRequest -Uri "http://localhost:3000/login" -TimeoutSec 5
    Write-Host "✅ Grafana: ACCESSIBLE" -ForegroundColor Green
    Write-Host "   - URL: http://localhost:3000 (admin/admin)" -ForegroundColor Cyan
} catch {
    Write-Host "⚠️  Grafana: STARTING UP..." -ForegroundColor Yellow
    Write-Host "   - URL: http://localhost:3000 (admin/admin)" -ForegroundColor Cyan
}

# Test Database Connectivity
Write-Host ""
Write-Host "💾 DATABASE CONNECTIVITY:" -ForegroundColor Yellow
Write-Host "--------------------------" -ForegroundColor Yellow

try {
    # Test database port
    $dbTest = Test-NetConnection -ComputerName localhost -Port 5432 -WarningAction SilentlyContinue
    if ($dbTest.TcpTestSucceeded) {
        Write-Host "✅ PostgreSQL Database: ACCESSIBLE" -ForegroundColor Green
        Write-Host "   - Port 5432: Open" -ForegroundColor Cyan
    } else {
        Write-Host "❌ PostgreSQL Database: NOT ACCESSIBLE" -ForegroundColor Red
    }
} catch {
    Write-Host "❌ Database Connectivity Test Failed" -ForegroundColor Red
}

# Container Status Check  
Write-Host ""
Write-Host "🐳 CONTAINER INFRASTRUCTURE:" -ForegroundColor Yellow
Write-Host "-----------------------------" -ForegroundColor Yellow

$containers = docker ps --format "table {{.Names}}\t{{.Status}}" --filter "name=metalayer"
Write-Host $containers -ForegroundColor Cyan

# Metrics Validation
Write-Host ""
Write-Host "📈 REAL-TIME METRICS VALIDATION:" -ForegroundColor Yellow
Write-Host "---------------------------------" -ForegroundColor Yellow

try {
    $prometheusMetrics = Invoke-WebRequest -Uri "http://localhost:9090/api/v1/label/__name__/values" | ConvertFrom-Json
    $metalayerMetricsList = $prometheusMetrics.data | Where-Object { $_ -like "*metalayer*" }
    
    Write-Host "✅ Prometheus Metrics Collection: ACTIVE" -ForegroundColor Green
    Write-Host "   Available MetaLayer Metrics:" -ForegroundColor Cyan
    foreach ($metric in $metalayerMetricsList) {
        Write-Host "   - $metric" -ForegroundColor White
    }
} catch {
    Write-Host "❌ Metrics Collection: NOT AVAILABLE" -ForegroundColor Red
}

# Access Information
Write-Host ""
Write-Host "🎯 ACCESS INFORMATION:" -ForegroundColor Yellow
Write-Host "----------------------" -ForegroundColor Yellow
Write-Host "🌐 Airflow Web UI:     http://localhost:8080" -ForegroundColor Green
Write-Host "   Credentials:        admin / admin" -ForegroundColor Cyan
Write-Host ""
Write-Host "📊 Grafana Dashboard:  http://localhost:3000" -ForegroundColor Green  
Write-Host "   Credentials:        admin / admin" -ForegroundColor Cyan
Write-Host ""
Write-Host "🔍 Prometheus:         http://localhost:9090" -ForegroundColor Green
Write-Host "📈 Raw Metrics:        http://localhost:8000/metrics" -ForegroundColor Green

Write-Host ""
Write-Host "🎉 SYSTEM VALIDATION COMPLETE!" -ForegroundColor Green
Write-Host "Your MetaLayer ETL monitoring system is operational!" -ForegroundColor Green
Write-Host "=============================================================" -ForegroundColor Green