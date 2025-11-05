# MetaLayer Backup Script for Windows PowerShell
# Automated backup solution for PostgreSQL and Airflow logs

param(
    [int]$RetentionDays = 30,
    [string]$BackupDir = "C:\MetaLayer\Backups",
    [string]$PostgresContainer = "metalayer_postgres"
)

# Error handling
$ErrorActionPreference = "Stop"

# Configuration
$Date = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = "$BackupDir\backup_$Date.log"

# Ensure backup directory exists
if (!(Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
}

# Logging function
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "$Timestamp [$Level] $Message"
    Write-Host $LogMessage
    Add-Content -Path $LogFile -Value $LogMessage
}

try {
    Write-Log "Starting MetaLayer backup process..." "INFO"

    # 1. PostgreSQL Database Backup
    Write-Log "Creating PostgreSQL backup..." "INFO"
    docker exec $PostgresContainer pg_dumpall -U postgres | Out-File -FilePath "$BackupDir\postgres_full_$Date.sql" -Encoding UTF8
    
    # 2. Individual Database Backups
    Write-Log "Creating individual database backups..." "INFO"
    $databases = @("airflow", "data_warehouse")
    foreach ($db in $databases) {
        try {
            docker exec $PostgresContainer pg_dump -U postgres $db | Out-File -FilePath "$BackupDir\postgres_${db}_$Date.sql" -Encoding UTF8
            Write-Log "Successfully backed up database: $db" "INFO"
        }
        catch {
            Write-Log "Warning: Failed to backup database $db - $($_.Exception.Message)" "WARN"
        }
    }

    # 3. Airflow Configuration Backup
    Write-Log "Backing up Airflow configuration..." "INFO"
    $configItems = @("dags", "include", "plugins", "airflow_settings.yaml", "docker-compose*.yml", ".env*")
    $configBackup = "$BackupDir\airflow_config_$Date.zip"
    
    # Create zip archive for configuration
    $compress = @{
        Path = $configItems | Where-Object { Test-Path $_ }
        CompressionLevel = "Optimal"
        DestinationPath = $configBackup
    }
    if ($compress.Path) {
        Compress-Archive @compress
        Write-Log "Configuration backup created: $configBackup" "INFO"
    }

    # 4. Logs Backup
    Write-Log "Backing up logs..." "INFO"
    if (Test-Path "logs") {
        $logsBackup = "$BackupDir\airflow_logs_$Date.zip"
        Compress-Archive -Path "logs\*" -DestinationPath $logsBackup -CompressionLevel Optimal
        Write-Log "Logs backup created: $logsBackup" "INFO"
    }

    # 5. Data Backup
    Write-Log "Backing up data files..." "INFO"
    if (Test-Path "data") {
        $dataBackup = "$BackupDir\data_files_$Date.zip"
        Compress-Archive -Path "data\*" -DestinationPath $dataBackup -CompressionLevel Optimal
        Write-Log "Data backup created: $dataBackup" "INFO"
    }

    # 6. Cleanup old backups
    Write-Log "Cleaning up old backups (older than $RetentionDays days)..." "INFO"
    $cutoffDate = (Get-Date).AddDays(-$RetentionDays)
    Get-ChildItem -Path $BackupDir -File | Where-Object { $_.CreationTime -lt $cutoffDate } | Remove-Item -Force
    Write-Log "Old backups cleaned up" "INFO"

    # 7. Backup verification
    Write-Log "Verifying backups..." "INFO"
    $backupFiles = Get-ChildItem -Path $BackupDir -Filter "*$Date*"
    foreach ($file in $backupFiles) {
        $size = [math]::Round($file.Length / 1MB, 2)
        Write-Log "✓ $($file.Name) ($size MB)" "INFO"
    }

    # 8. Generate backup report
    Write-Log "Generating backup report..." "INFO"
    $reportPath = "$BackupDir\backup_report_$Date.txt"
    
    $report = @"
MetaLayer Backup Report
======================
Date: $(Get-Date)
Backup ID: $Date
PowerShell Version: $($PSVersionTable.PSVersion)

Files Created:
$(($backupFiles | Format-Table Name, @{Name="Size (MB)"; Expression={[math]::Round($_.Length / 1MB, 2)}} | Out-String))

Backup Directory Usage:
$((Get-ChildItem $BackupDir | Measure-Object -Property Length -Sum | ForEach-Object { [math]::Round($_.Sum / 1MB, 2) }) MB)

Container Status:
$(docker ps --format "table {{.Names}}`t{{.Status}}`t{{.Image}}" | Out-String)

Database Status:
$(try { docker exec $PostgresContainer psql -U postgres -c "SELECT datname, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datistemplate = false;" } catch { "Database status unavailable" })
"@

    Set-Content -Path $reportPath -Value $report
    Write-Log "Backup completed successfully!" "INFO"
    Write-Log "Backup report: $reportPath" "INFO"

} catch {
    Write-Log "ERROR: Backup failed - $($_.Exception.Message)" "ERROR"
    throw
}

# Optional: Send notification via email (requires configuration)
# Send-MailMessage -To "admin@company.com" -From "backup@company.com" -Subject "MetaLayer Backup Success" -Body "Backup completed successfully at $(Get-Date)" -SmtpServer "smtp.company.com"