#!/usr/bin/env pwsh
# MetaLayer ETL Shutdown Script
# Gracefully stop all services

Write-Host "🛑 Stopping MetaLayer ETL Pipeline..." -ForegroundColor Yellow
Write-Host "Time: $(Get-Date)" -ForegroundColor Gray

# Change to script directory
$ScriptPath = Split-Path -Parent $MyInvocation.MyCommand.Definition  
Set-Location $ScriptPath

# Stop services gracefully
Write-Host "`n📦 Stopping Docker services..." -ForegroundColor Yellow
docker-compose -f docker-compose-fast.yml down

Write-Host "`n✅ MetaLayer ETL stopped successfully!" -ForegroundColor Green
Write-Host "💾 All data has been preserved in Docker volumes" -ForegroundColor Gray

Write-Host "`n📋 To restart:" -ForegroundColor White
Write-Host "   • Run: .\start-etl.ps1" -ForegroundColor Gray
Write-Host "   • Or:  docker-compose -f docker-compose-fast.yml up -d" -ForegroundColor Gray