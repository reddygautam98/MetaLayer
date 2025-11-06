# =====================================================
# ETL PIPELINE MONITORING & TROUBLESHOOTING SCRIPT
# PowerShell script for monitoring and diagnosing issues
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("health", "performance", "logs", "debug", "backup", "restore")]
    [string]$Operation = "health"
)

$Green = "Green"
$Red = "Red"
$Yellow = "Yellow"
$Blue = "Cyan"

function Write-Header {
    param([string]$Title)
    Write-Host "`n" -NoNewline
    Write-Host "=" * 60 -ForegroundColor $Blue
    Write-Host " $Title" -ForegroundColor $Blue
    Write-Host "=" * 60 -ForegroundColor $Blue
}

function Test-ServiceHealth {
    Write-Header "ETL Pipeline Health Check"
    
    $services = @(
        @{Name="PostgreSQL"; Container="etl_postgres"; Port=5432; Type="tcp"},
        @{Name="Redis"; Container="etl_redis"; Port=6379; Type="tcp"},
        @{Name="Airflow Webserver"; Container="etl_airflow_webserver"; Port=8080; Type="http"},
        @{Name="Airflow Scheduler"; Container="etl_airflow_scheduler"; Port=$null; Type="process"},
        @{Name="Airflow Worker"; Container="etl_airflow_worker"; Port=$null; Type="process"},
        @{Name="Prometheus"; Container="etl_prometheus"; Port=9090; Type="http"},
        @{Name="Grafana"; Container="etl_grafana"; Port=3000; Type="http"}
    )
    
    foreach ($service in $services) {
        Write-Host "`n🔍 Checking $($service.Name)..." -ForegroundColor $Blue
        
        # Check if container is running
        $containerStatus = docker inspect $service.Container --format='{{.State.Status}}' 2>$null
        if ($containerStatus -eq "running") {
            Write-Host "  ✅ Container Status: Running" -ForegroundColor $Green
            
            # Additional health checks based on service type
            switch ($service.Type) {
                "http" {
                    try {
                        $url = "http://localhost:$($service.Port)"
                        $response = Invoke-WebRequest -Uri $url -TimeoutSec 5 -UseBasicParsing
                        if ($response.StatusCode -eq 200) {
                            Write-Host "  ✅ HTTP Health: OK ($url)" -ForegroundColor $Green
                        }
                    } catch {
                        Write-Host "  ❌ HTTP Health: Failed ($url)" -ForegroundColor $Red
                    }
                }
                "tcp" {
                    $tcpTest = Test-NetConnection -ComputerName "localhost" -Port $service.Port -InformationLevel Quiet
                    if ($tcpTest) {
                        Write-Host "  ✅ Port $($service.Port): Open" -ForegroundColor $Green
                    } else {
                        Write-Host "  ❌ Port $($service.Port): Closed" -ForegroundColor $Red
                    }
                }
                "process" {
                    $processCount = docker exec $service.Container pgrep -f airflow | Measure-Object | Select-Object -ExpandProperty Count
                    if ($processCount -gt 0) {
                        Write-Host "  ✅ Process Health: $processCount airflow processes running" -ForegroundColor $Green
                    } else {
                        Write-Host "  ❌ Process Health: No airflow processes found" -ForegroundColor $Red
                    }
                }
            }
            
            # Check memory and CPU usage
            $stats = docker stats $service.Container --no-stream --format "{{.CPUPerc}},{{.MemUsage}}" 2>$null
            if ($stats) {
                $cpu, $memory = $stats -split ","
                Write-Host "  📊 Resources: CPU $cpu, Memory $memory" -ForegroundColor $Blue
            }
            
        } elseif ($containerStatus -eq "exited") {
            Write-Host "  ❌ Container Status: Exited" -ForegroundColor $Red
            $exitCode = docker inspect $service.Container --format='{{.State.ExitCode}}' 2>$null
            Write-Host "  ❌ Exit Code: $exitCode" -ForegroundColor $Red
        } else {
            Write-Host "  ❌ Container Status: Not Found or Not Running" -ForegroundColor $Red
        }
    }
    
    # Check DAG status
    Write-Host "`n🔍 Checking Airflow DAGs..." -ForegroundColor $Blue
    try {
        $dagList = docker exec etl_airflow_webserver airflow dags list --output json 2>$null | ConvertFrom-Json
        if ($dagList) {
            $totalDags = $dagList.Count
            $pausedDags = ($dagList | Where-Object { $_.is_paused -eq $true }).Count
            $activeDags = $totalDags - $pausedDags
            Write-Host "  ✅ Total DAGs: $totalDags (Active: $activeDags, Paused: $pausedDags)" -ForegroundColor $Green
        }
    } catch {
        Write-Host "  ❌ Failed to retrieve DAG status" -ForegroundColor $Red
    }
}

function Show-Performance {
    Write-Header "ETL Pipeline Performance Metrics"
    
    Write-Host "📊 Container Resource Usage:" -ForegroundColor $Blue
    docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}"
    
    Write-Host "`n💾 Docker System Usage:" -ForegroundColor $Blue  
    docker system df
    
    Write-Host "`n🏃 Recent DAG Runs:" -ForegroundColor $Blue
    try {
        docker exec etl_airflow_webserver airflow dags list-runs --limit 10 --output table
    } catch {
        Write-Host "Unable to retrieve DAG run information" -ForegroundColor $Yellow
    }
    
    Write-Host "`n📈 Database Connection Stats:" -ForegroundColor $Blue
    try {
        $dbStats = docker exec etl_postgres psql -U postgres -d airflow -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';" -t
        Write-Host "Active Database Connections: $($dbStats.Trim())" -ForegroundColor $Green
    } catch {
        Write-Host "Unable to retrieve database connection stats" -ForegroundColor $Yellow
    }
}

function Show-DetailedLogs {
    Write-Header "ETL Pipeline Detailed Logs"
    
    $services = @("etl_postgres", "etl_redis", "etl_airflow_webserver", "etl_airflow_scheduler", "etl_airflow_worker", "etl_prometheus", "etl_grafana")
    
    foreach ($service in $services) {
        Write-Host "`n📋 Logs for $service (last 20 lines):" -ForegroundColor $Blue
        Write-Host "-" * 50 -ForegroundColor $Blue
        docker logs --tail 20 $service 2>&1
    }
}

function Start-DebugSession {
    Write-Header "ETL Pipeline Debug Session"
    
    Write-Host "🔧 Starting interactive debug session..." -ForegroundColor $Blue
    Write-Host "Available debug commands:" -ForegroundColor $Green
    Write-Host "  1. Check Airflow webserver logs: docker logs -f etl_airflow_webserver" -ForegroundColor $Yellow
    Write-Host "  2. Check scheduler logs: docker logs -f etl_airflow_scheduler" -ForegroundColor $Yellow
    Write-Host "  3. Connect to database: docker exec -it etl_postgres psql -U postgres -d airflow" -ForegroundColor $Yellow
    Write-Host "  4. Check Airflow CLI: docker exec -it etl_airflow_webserver airflow --help" -ForegroundColor $Yellow
    Write-Host "  5. Test DAG: docker exec etl_airflow_webserver airflow dags test [dag_id] [execution_date]" -ForegroundColor $Yellow
    
    $choice = Read-Host "`nEnter command number (1-5) or 'exit' to quit"
    
    switch ($choice) {
        "1" { docker logs -f etl_airflow_webserver }
        "2" { docker logs -f etl_airflow_scheduler }
        "3" { docker exec -it etl_postgres psql -U postgres -d airflow }
        "4" { docker exec -it etl_airflow_webserver airflow --help }
        "5" { 
            $dagId = Read-Host "Enter DAG ID"
            $execDate = Read-Host "Enter execution date (YYYY-MM-DD)"
            docker exec etl_airflow_webserver airflow dags test $dagId $execDate
        }
        "exit" { Write-Host "Exiting debug session..." -ForegroundColor $Blue }
        default { Write-Host "Invalid choice" -ForegroundColor $Red }
    }
}

function Backup-Data {
    Write-Header "ETL Pipeline Data Backup"
    
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $backupDir = "backups\backup_$timestamp"
    
    Write-Host "Creating backup directory: $backupDir" -ForegroundColor $Blue
    New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
    
    # Backup database
    Write-Host "📦 Backing up PostgreSQL database..." -ForegroundColor $Blue
    docker exec etl_postgres pg_dump -U postgres -d airflow > "$backupDir\airflow_db.sql"
    
    # Backup DAG logs
    Write-Host "📦 Backing up Airflow logs..." -ForegroundColor $Blue
    docker cp etl_airflow_webserver:/opt/airflow/logs "$backupDir\airflow_logs"
    
    # Backup configuration
    Write-Host "📦 Backing up configuration files..." -ForegroundColor $Blue
    Copy-Item ".env" "$backupDir\.env"
    Copy-Item "docker-compose.yml" "$backupDir\docker-compose.yml"
    
    # Create backup info file
    $backupInfo = @"
ETL Pipeline Backup Information
===============================
Backup Date: $(Get-Date)
Backup Directory: $backupDir
Database: airflow_db.sql
Logs: airflow_logs/
Configuration: .env, docker-compose.yml

Restore Instructions:
1. Stop all services: docker compose down
2. Restore database: docker exec -i etl_postgres psql -U postgres -d airflow < airflow_db.sql  
3. Restore logs: docker cp airflow_logs etl_airflow_webserver:/opt/airflow/
4. Restart services: docker compose up -d
"@
    
    Set-Content -Path "$backupDir\README.txt" -Value $backupInfo
    
    Write-Host "✅ Backup completed successfully!" -ForegroundColor $Green
    Write-Host "📁 Backup location: $backupDir" -ForegroundColor $Blue
}

function Restore-Data {
    Write-Header "ETL Pipeline Data Restore"
    
    $backupDirs = Get-ChildItem -Path "backups" -Directory | Sort-Object CreationTime -Descending
    
    if ($backupDirs.Count -eq 0) {
        Write-Host "❌ No backup directories found in 'backups' folder" -ForegroundColor $Red
        return
    }
    
    Write-Host "Available backups:" -ForegroundColor $Blue
    for ($i = 0; $i -lt $backupDirs.Count; $i++) {
        Write-Host "  $($i + 1). $($backupDirs[$i].Name)" -ForegroundColor $Yellow
    }
    
    $choice = Read-Host "Select backup to restore (1-$($backupDirs.Count))"
    
    if ($choice -match "^\d+$" -and [int]$choice -le $backupDirs.Count -and [int]$choice -gt 0) {
        $selectedBackup = $backupDirs[[int]$choice - 1]
        $backupPath = $selectedBackup.FullName
        
        Write-Host "⚠️  This will overwrite current data. Are you sure? (yes/no)" -ForegroundColor $Yellow
        $confirmation = Read-Host
        
        if ($confirmation -eq "yes") {
            Write-Host "📋 Restoring from: $backupPath" -ForegroundColor $Blue
            
            # Stop services
            Write-Host "⏹️  Stopping services..." -ForegroundColor $Blue
            docker compose down
            
            # Start only database
            Write-Host "🚀 Starting database..." -ForegroundColor $Blue  
            docker compose up -d postgres
            Start-Sleep -Seconds 10
            
            # Restore database
            if (Test-Path "$backupPath\airflow_db.sql") {
                Write-Host "📦 Restoring database..." -ForegroundColor $Blue
                Get-Content "$backupPath\airflow_db.sql" | docker exec -i etl_postgres psql -U postgres -d airflow
            }
            
            # Start all services
            Write-Host "🚀 Starting all services..." -ForegroundColor $Blue
            docker compose up -d
            
            Write-Host "✅ Restore completed successfully!" -ForegroundColor $Green
        } else {
            Write-Host "❌ Restore cancelled" -ForegroundColor $Red
        }
    } else {
        Write-Host "❌ Invalid selection" -ForegroundColor $Red
    }
}

# Main script execution
switch ($Operation.ToLower()) {
    "health" { Test-ServiceHealth }
    "performance" { Show-Performance }
    "logs" { Show-DetailedLogs }
    "debug" { Start-DebugSession }
    "backup" { Backup-Data }
    "restore" { Restore-Data }
    default {
        Write-Host "Invalid operation: $Operation" -ForegroundColor $Red
        Write-Host "Valid operations: health, performance, logs, debug, backup, restore" -ForegroundColor $Yellow
    }
}