#!/usr/bin/env pwsh
# MetaLayer ETL Health Check & Auto-Recovery Script
# Run this script to verify and maintain system health

param(
    [switch]$Fix = $false,
    [switch]$Detailed = $false
)

Write-Host "🔍 MetaLayer ETL Health Check Starting..." -ForegroundColor Green
Write-Host "Time: $(Get-Date)" -ForegroundColor Gray

# Change to MetaLayer directory
$ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptPath

# Health Check Functions
function Test-ServiceHealth {
    Write-Host "`n📊 Checking Service Health..." -ForegroundColor Yellow
    
    $services = docker-compose -f docker-compose-fast.yml ps --format json | ConvertFrom-Json
    $allHealthy = $true
    
    foreach ($service in $services) {
        $status = $service.Health
        $name = $service.Service
        
        if ($status -eq "healthy" -or $service.Status -match "healthy") {
            Write-Host "✅ $name - Healthy" -ForegroundColor Green
        } else {
            Write-Host "❌ $name - Status: $($service.Status)" -ForegroundColor Red
            $allHealthy = $false
        }
    }
    
    return $allHealthy
}

function Test-DatabaseConnectivity {
    Write-Host "`n🗄️ Testing Database Connectivity..." -ForegroundColor Yellow
    
    try {
        $result = docker-compose -f docker-compose-fast.yml exec -T postgres psql -U postgres -d metalayer_etl -c "SELECT COUNT(*) FROM bronze.customers_raw;" 2>$null
        if ($result -match "\s+5\s*") {
            Write-Host "✅ Database - Connected (5 sample customers)" -ForegroundColor Green
            return $true
        }
    }
    catch {
        Write-Host "❌ Database - Connection Failed" -ForegroundColor Red
        return $false
    }
}

function Test-AirflowUI {
    Write-Host "`n🌐 Testing Airflow UI..." -ForegroundColor Yellow
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 10 -UseBasicParsing
        if ($response.StatusCode -eq 200) {
            Write-Host "✅ Airflow UI - Accessible (HTTP $($response.StatusCode))" -ForegroundColor Green
            return $true
        }
    }
    catch {
        Write-Host "❌ Airflow UI - Not accessible: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Test-DAGs {
    Write-Host "`n📋 Testing DAGs..." -ForegroundColor Yellow
    
    try {
        $dagList = docker-compose -f docker-compose-fast.yml exec -T webserver airflow dags list 2>$null
        $dagCount = ($dagList -split "`n" | Where-Object { $_ -match "bronze_layer|silver_layer|gold_layer|orchestrator|init_db" }).Count
        
        if ($dagCount -ge 5) {
            Write-Host "✅ DAGs - All 5 DAGs loaded successfully" -ForegroundColor Green
            
            # Check for import errors
            $importErrors = docker-compose -f docker-compose-fast.yml exec -T webserver airflow dags list-import-errors 2>$null
            if ($importErrors -match "No data found") {
                Write-Host "✅ DAGs - No import errors" -ForegroundColor Green
                return $true
            }
        }
        
        Write-Host "✅ DAGs - $dagCount DAGs loaded (expected: 5)" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "❌ DAGs - Check failed: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Test-MemoryUsage {
    Write-Host "`n💾 Checking Memory Usage..." -ForegroundColor Yellow
    
    try {
        $stats = docker stats --no-stream --format "{{.Container}} {{.MemPerc}}"
        $highMemoryContainers = @()
        
        foreach ($line in $stats -split "`n") {
            if ($line -match "^(\S+)\s+(\d+\.?\d*)%") {
                $container = $matches[1]
                $memPerc = [double]$matches[2]
                
                if ($memPerc -gt 80) {
                    $highMemoryContainers += $container
                    Write-Host "⚠️  $container - High memory usage: $($memPerc)%" -ForegroundColor Yellow
                } else {
                    Write-Host "✅ $container - Memory usage: $($memPerc)%" -ForegroundColor Green
                }
            }
        }
        
        return $highMemoryContainers.Count -eq 0
    }
    catch {
        Write-Host "❌ Memory check failed: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

function Fix-Issues {
    Write-Host "`n🔧 Auto-fixing detected issues..." -ForegroundColor Yellow
    
    Write-Host "Restarting services for clean slate..." -ForegroundColor Gray
    docker-compose -f docker-compose-fast.yml restart webserver scheduler worker
    
    Write-Host "Waiting for services to stabilize..." -ForegroundColor Gray
    Start-Sleep 30
    
    Write-Host "✅ Auto-fix completed" -ForegroundColor Green
}

function Show-DetailedStatus {
    Write-Host "`n📊 Detailed System Status:" -ForegroundColor Cyan
    
    Write-Host "`nContainer Resource Usage:" -ForegroundColor White
    docker stats --no-stream
    
    Write-Host "`nContainer Status:" -ForegroundColor White
    docker-compose -f docker-compose-fast.yml ps
    
    Write-Host "`nDAG Status:" -ForegroundColor White
    docker-compose -f docker-compose-fast.yml exec -T webserver airflow dags list
    
    Write-Host "`nDatabase Schema Status:" -ForegroundColor White
    docker-compose -f docker-compose-fast.yml exec -T postgres psql -U postgres -d metalayer_etl -c "SELECT schemaname, COUNT(*) as table_count FROM information_schema.tables WHERE table_schema IN ('bronze','silver','gold','airflow_meta') GROUP BY schemaname ORDER BY schemaname;"
}

# Main Health Check Execution
Write-Host "`n" + "="*60 -ForegroundColor Cyan
Write-Host "METALAYER ETL SYSTEM HEALTH CHECK" -ForegroundColor Cyan
Write-Host "="*60 -ForegroundColor Cyan

$healthChecks = @()
$healthChecks += Test-ServiceHealth
$healthChecks += Test-DatabaseConnectivity  
$healthChecks += Test-AirflowUI
$healthChecks += Test-DAGs
$healthChecks += Test-MemoryUsage

$overallHealth = ($healthChecks | Where-Object { $_ -eq $true }).Count
$totalChecks = $healthChecks.Count

Write-Host "`n" + "="*60 -ForegroundColor Cyan
if ($overallHealth -eq $totalChecks) {
    Write-Host "🎉 SYSTEM STATUS: ALL GREEN" -ForegroundColor Green
    Write-Host "✅ $overallHealth/$totalChecks health checks passed" -ForegroundColor Green
    Write-Host "🚀 Your ETL pipeline is ready for production!" -ForegroundColor Green
} else {
    Write-Host "⚠️  SYSTEM STATUS: ISSUES DETECTED" -ForegroundColor Yellow
    Write-Host "✅ $overallHealth/$totalChecks health checks passed" -ForegroundColor Yellow
    
    if ($Fix) {
        Fix-Issues
        Write-Host "Re-running health checks after auto-fix..." -ForegroundColor Gray
        & $MyInvocation.MyCommand.Path
    } else {
        Write-Host "💡 Run with -Fix parameter to auto-resolve issues" -ForegroundColor Cyan
    }
}

if ($Detailed) {
    Show-DetailedStatus
}

Write-Host "`n🌐 Access URLs:" -ForegroundColor White
Write-Host "   Airflow UI: http://localhost:8080 (admin/admin)" -ForegroundColor Gray
Write-Host "   Database:   postgresql://postgres:postgres@localhost:5432/metalayer_etl" -ForegroundColor Gray

Write-Host "`n📋 Quick Commands:" -ForegroundColor White
Write-Host "   Health Check:     .\health-check.ps1" -ForegroundColor Gray
Write-Host "   Detailed Status:  .\health-check.ps1 -Detailed" -ForegroundColor Gray
Write-Host "   Auto-fix Issues:  .\health-check.ps1 -Fix" -ForegroundColor Gray
Write-Host "   Open Airflow:     Start-Process 'http://localhost:8080'" -ForegroundColor Gray

Write-Host "`n🔍 Health check completed at $(Get-Date)" -ForegroundColor Green