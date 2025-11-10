# =====================================================
# METALAYER SYSTEM VALIDATION WITH CORRECT CREDENTIALS
# Complete end-to-end validation of all services
# =====================================================

Write-Host "🚀 MetaLayer System Validation - Complete Check" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan

# Service endpoints with correct credentials
$services = @{
    "Airflow" = @{
        "url" = "http://localhost:8080/health"
        "ui" = "http://localhost:8080"
        "credentials" = "admin/admin"
        "description" = "ETL Pipeline Web Interface"
    }
    "Grafana" = @{
        "url" = "http://localhost:3000/api/health" 
        "ui" = "http://localhost:3000"
        "credentials" = "admin/Litureddy098@"
        "description" = "Monitoring Dashboard"
    }
    "Prometheus" = @{
        "url" = "http://localhost:9090/-/healthy"
        "ui" = "http://localhost:9090"
        "credentials" = "No authentication"
        "description" = "Metrics Collection"
    }
    "PostgreSQL" = @{
        "url" = "localhost:5432"
        "credentials" = "postgres/etl_pipeline_2024"
        "description" = "Data Warehouse"
    }
}

# Docker container validation
Write-Host "`n📦 Container Status Check:" -ForegroundColor Yellow
$containers = docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
Write-Host $containers

Write-Host "`n🔍 Service Health Validation:" -ForegroundColor Yellow

foreach ($serviceName in $services.Keys) {
    $service = $services[$serviceName]
    Write-Host "`n🔸 Testing $serviceName..." -ForegroundColor White
    
    if ($serviceName -eq "PostgreSQL") {
        # Test PostgreSQL connection
        try {
            docker exec metalayer-postgres-1 pg_isready -U postgres -d airflow
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  ✅ PostgreSQL is ready" -ForegroundColor Green
                Write-Host "  📍 Connection: $($service.url)" -ForegroundColor Gray
                Write-Host "  🔑 Credentials: $($service.credentials)" -ForegroundColor Gray
            } else {
                Write-Host "  ❌ PostgreSQL connection failed" -ForegroundColor Red
            }
        } catch {
            Write-Host "  ❌ PostgreSQL test error: $($_.Exception.Message)" -ForegroundColor Red
        }
    } else {
        # Test HTTP services
        try {
            $response = Invoke-WebRequest -Uri $service.url -Method Get -TimeoutSec 10 -UseBasicParsing
            if ($response.StatusCode -eq 200) {
                Write-Host "  ✅ $serviceName is healthy (HTTP $($response.StatusCode))" -ForegroundColor Green
                Write-Host "  🌐 Web UI: $($service.ui)" -ForegroundColor Gray
                Write-Host "  🔑 Credentials: $($service.credentials)" -ForegroundColor Gray
                Write-Host "  📋 Description: $($service.description)" -ForegroundColor Gray
            } else {
                Write-Host "  ⚠️  $serviceName responded with HTTP $($response.StatusCode)" -ForegroundColor Yellow
            }
        } catch {
            Write-Host "  ❌ $serviceName is not responding: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

# Grafana datasource validation
Write-Host "`n📊 Grafana Datasource Validation:" -ForegroundColor Yellow
try {
    $grafanaHeaders = @{
        'Authorization' = 'Basic ' + [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("admin:Litureddy098@"))
        'Content-Type' = 'application/json'
    }
    
    $datasources = Invoke-RestMethod -Uri "http://localhost:3000/api/datasources" -Headers $grafanaHeaders -Method Get
    
    if ($datasources.Count -gt 0) {
        Write-Host "  ✅ Found $($datasources.Count) configured datasource(s):" -ForegroundColor Green
        foreach ($ds in $datasources) {
            Write-Host "    • $($ds.name) ($($ds.type)) - $($ds.url)" -ForegroundColor Gray
        }
    } else {
        Write-Host "  ⚠️  No datasources configured. Run datasource setup:" -ForegroundColor Yellow
        Write-Host "    .\scripts\setup-grafana-datasources.ps1 -Password 'Litureddy098@'" -ForegroundColor Gray
    }
} catch {
    Write-Host "  ❌ Could not connect to Grafana API: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "  💡 Try: .\scripts\grafana-complete.ps1 -Action datasources" -ForegroundColor Yellow
}

# ETL DAG validation
Write-Host "`n🔄 ETL Pipeline (DAG) Status:" -ForegroundColor Yellow
try {
    $airflowHeaders = @{
        'Authorization' = 'Basic ' + [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("admin:admin"))
        'Content-Type' = 'application/json'
    }
    
    $dags = Invoke-RestMethod -Uri "http://localhost:8080/api/v1/dags" -Headers $airflowHeaders -Method Get
    
    $importantDags = @("bronze_layer_docker", "silver_layer_docker", "gold_layer_docker", "master_orchestrator_docker")
    
    Write-Host "  📋 ETL DAG Status:" -ForegroundColor White
    foreach ($dagId in $importantDags) {
        $dag = $dags.dags | Where-Object { $_.dag_id -eq $dagId }
        if ($dag) {
            $status = if ($dag.is_paused) { "⏸️  PAUSED" } else { "▶️  ACTIVE" }
            Write-Host "    • $dagId : $status" -ForegroundColor Gray
        } else {
            Write-Host "    • $dagId : ❌ NOT FOUND" -ForegroundColor Red
        }
    }
} catch {
    Write-Host "  ❌ Could not connect to Airflow API: $($_.Exception.Message)" -ForegroundColor Red
}

# Summary and next steps
Write-Host "`n🎯 SYSTEM STATUS SUMMARY:" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "✅ All services should be accessible with the credentials above" -ForegroundColor Green
Write-Host "✅ Default Grafana password updated to: Litureddy098@" -ForegroundColor Green
Write-Host "✅ Automated datasource configuration available" -ForegroundColor Green
Write-Host "" 
Write-Host "📚 Quick Access Links:" -ForegroundColor White
Write-Host "  • Airflow UI:    http://localhost:8080 (admin/admin)" -ForegroundColor Gray
Write-Host "  • Grafana UI:    http://localhost:3000 (admin/Litureddy098@)" -ForegroundColor Gray  
Write-Host "  • Prometheus UI: http://localhost:9090" -ForegroundColor Gray
Write-Host ""
Write-Host "🛠️  Management Commands:" -ForegroundColor White
Write-Host "  • Setup Grafana datasources: .\scripts\setup-grafana-datasources.ps1" -ForegroundColor Gray
Write-Host "  • Grafana management: .\scripts\grafana-complete.ps1 -Action [setup|health|datasources]" -ForegroundColor Gray
Write-Host "  • Full system restart: docker-compose down && docker-compose up -d" -ForegroundColor Gray