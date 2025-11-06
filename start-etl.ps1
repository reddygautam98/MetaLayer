#!/usr/bin/env pwsh
# MetaLayer ETL Quick Start Script
# One command to start everything and verify it's working

Write-Host "🚀 Starting MetaLayer ETL Pipeline..." -ForegroundColor Green
Write-Host "Time: $(Get-Date)" -ForegroundColor Gray

# Change to script directory  
$ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $ScriptPath

# Start services
Write-Host "`n📦 Starting Docker services..." -ForegroundColor Yellow
docker-compose -f docker-compose-fast.yml up -d

# Wait for services to initialize
Write-Host "`n⏳ Waiting for services to initialize (60 seconds)..." -ForegroundColor Yellow
$counter = 0
while ($counter -lt 60) {
    $dots = "." * (($counter % 4) + 1)
    Write-Host "`r   Initializing$dots" -NoNewline -ForegroundColor Gray
    Start-Sleep 1
    $counter++
}
Write-Host ""

# Run health check
Write-Host "`n🔍 Running system health check..." -ForegroundColor Yellow
.\health-check.ps1

Write-Host "`n🌐 Opening Airflow UI..." -ForegroundColor Yellow
Start-Sleep 2
Start-Process "http://localhost:8080"

Write-Host "`n✅ MetaLayer ETL startup completed!" -ForegroundColor Green
Write-Host "🎯 Your ETL pipeline is ready for use!" -ForegroundColor Cyan

Write-Host "`n📋 What you can do now:" -ForegroundColor White
Write-Host "   • Login to Airflow UI (admin/admin)" -ForegroundColor Gray  
Write-Host "   • Trigger 'master_etl_orchestrator' to run full pipeline" -ForegroundColor Gray
Write-Host "   • Monitor DAG executions in real-time" -ForegroundColor Gray
Write-Host "   • Query results in PostgreSQL database" -ForegroundColor Gray

Write-Host "`n🛟 If issues occur:" -ForegroundColor White
Write-Host "   • Run: .\health-check.ps1 -Fix" -ForegroundColor Gray
Write-Host "   • Check: .\health-check.ps1 -Detailed" -ForegroundColor Gray