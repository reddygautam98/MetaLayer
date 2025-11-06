# =====================================================
# ETL PIPELINE DOCKER DEPLOYMENT SCRIPT
# Comprehensive PowerShell script for Docker deployment
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("start", "stop", "restart", "status", "logs", "cleanup", "build")]
    [string]$Action = "start",
    
    [Parameter(Mandatory=$false)]
    [string]$Service = "all",
    
    [Parameter(Mandatory=$false)]
    [switch]$Detach = $true,
    
    [Parameter(Mandatory=$false)]
    [switch]$Build = $false
)

# Script configuration
$ScriptName = "ETL Pipeline Docker Manager"
$ComposeFile = "docker-compose.yml"
$EnvFile = ".env"

# Colors for output
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

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor $Green
}

function Write-Error {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor $Red
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠️  $Message" -ForegroundColor $Yellow
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ️  $Message" -ForegroundColor $Blue
}

function Test-Prerequisites {
    Write-Header "Checking Prerequisites"
    
    # Check Docker
    try {
        $dockerVersion = docker --version
        Write-Success "Docker found: $dockerVersion"
    }
    catch {
        Write-Error "Docker not found. Please install Docker Desktop."
        return $false
    }
    
    # Check Docker Compose
    try {
        $composeVersion = docker compose version
        Write-Success "Docker Compose found: $composeVersion"
    }
    catch {
        Write-Error "Docker Compose not found. Please install Docker Compose."
        return $false
    }
    
    # Check files exist
    if (-not (Test-Path $ComposeFile)) {
        Write-Error "docker-compose.yml not found in current directory."
        return $false
    }
    Write-Success "docker-compose.yml found"
    
    if (-not (Test-Path $EnvFile)) {
        Write-Warning ".env file not found. Using default environment variables."
    } else {
        Write-Success ".env file found"
    }
    
    return $true
}

function Start-Services {
    Write-Header "Starting ETL Pipeline Services"
    
    if (-not (Test-Prerequisites)) {
        Write-Error "Prerequisites check failed. Aborting."
        return
    }
    
    Write-Info "Generating Airflow Fernet key..."
    $fernetKey = python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    if ($fernetKey) {
        $envContent = Get-Content $EnvFile -Raw
        $envContent = $envContent -replace "AIRFLOW_FERNET_KEY=.*", "AIRFLOW_FERNET_KEY=$fernetKey"
        Set-Content $EnvFile -Value $envContent
        Write-Success "Fernet key generated and updated in .env file"
    }
    
    $buildFlag = if ($Build) { "--build" } else { "" }
    $detachFlag = if ($Detach) { "-d" } else { "" }
    
    Write-Info "Starting Docker Compose services..."
    
    if ($Service -eq "all") {
        $command = "docker compose $buildFlag up $detachFlag"
    } else {
        $command = "docker compose $buildFlag up $detachFlag $Service"
    }
    
    Write-Info "Running: $command"
    Invoke-Expression $command
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Services started successfully!"
        Write-Info "Access points:"
        Write-Host "  📊 Airflow Web UI:    http://localhost:8080 (admin/admin)" -ForegroundColor $Blue
        Write-Host "  📈 Grafana:           http://localhost:3000 (admin/admin_secure_2024)" -ForegroundColor $Blue  
        Write-Host "  🔍 Prometheus:        http://localhost:9090" -ForegroundColor $Blue
        Write-Host "  💾 PostgreSQL:        localhost:5432 (postgres/etl_secure_2024!)" -ForegroundColor $Blue
        
        # Wait for services to be healthy
        Write-Info "Waiting for services to be healthy..."
        Start-Sleep -Seconds 30
        Show-Status
    } else {
        Write-Error "Failed to start services. Check the logs for details."
    }
}

function Stop-Services {
    Write-Header "Stopping ETL Pipeline Services"
    
    if ($Service -eq "all") {
        Write-Info "Stopping all services..."
        docker compose down
    } else {
        Write-Info "Stopping service: $Service"
        docker compose stop $Service
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Services stopped successfully!"
    } else {
        Write-Error "Failed to stop services."
    }
}

function Restart-Services {
    Write-Header "Restarting ETL Pipeline Services"
    Stop-Services
    Start-Sleep -Seconds 5
    Start-Services
}

function Show-Status {
    Write-Header "ETL Pipeline Services Status"
    
    Write-Info "Container Status:"
    docker compose ps
    
    Write-Info "`nContainer Health Status:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | Where-Object { $_ -like "*etl_*" }
    
    Write-Info "`nService URLs:"
    $services = @(
        @{Name="Airflow Web UI"; URL="http://localhost:8080"; Status=""},
        @{Name="Grafana"; URL="http://localhost:3000"; Status=""},
        @{Name="Prometheus"; URL="http://localhost:9090"; Status=""},
        @{Name="PostgreSQL"; URL="localhost:5432"; Status=""}
    )
    
    foreach ($service in $services) {
        $status = "❓"
        if ($service.URL -like "http*") {
            try {
                $response = Invoke-WebRequest -Uri $service.URL -TimeoutSec 5 -UseBasicParsing
                $status = if ($response.StatusCode -eq 200) { "✅" } else { "❌" }
            } catch {
                $status = "❌"
            }
        } else {
            # Test database connection
            $status = "❓"
        }
        Write-Host "  $status $($service.Name): $($service.URL)" -ForegroundColor $Blue
    }
}

function Show-Logs {
    param([string]$ServiceName = "")
    
    Write-Header "Service Logs"
    
    if ($ServiceName -and $ServiceName -ne "all") {
        Write-Info "Showing logs for service: $ServiceName"
        docker compose logs -f --tail=100 $ServiceName
    } else {
        Write-Info "Showing logs for all services (last 50 lines each):"
        docker compose logs --tail=50
    }
}

function Invoke-Cleanup {
    Write-Header "Cleaning Up ETL Pipeline"
    
    Write-Warning "This will stop and remove all containers, networks, and volumes."
    $confirmation = Read-Host "Are you sure? Type 'yes' to continue"
    
    if ($confirmation -eq "yes") {
        Write-Info "Stopping and removing containers..."
        docker compose down -v --remove-orphans
        
        Write-Info "Removing unused Docker resources..."
        docker system prune -f
        
        Write-Success "Cleanup completed!"
        
        # Show space reclaimed
        Write-Info "Docker disk usage:"
        docker system df
    } else {
        Write-Info "Cleanup cancelled."
    }
}

function Build-Images {
    Write-Header "Building ETL Pipeline Images"
    
    Write-Info "Building Docker images..."
    docker compose build --no-cache
    
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Images built successfully!"
    } else {
        Write-Error "Failed to build images."
    }
}

# Main script execution
Write-Header $ScriptName

switch ($Action.ToLower()) {
    "start" {
        Start-Services
    }
    "stop" {
        Stop-Services
    }
    "restart" {
        Restart-Services
    }
    "status" {
        Show-Status
    }
    "logs" {
        Show-Logs -ServiceName $Service
    }
    "cleanup" {
        Invoke-Cleanup
    }
    "build" {
        Build-Images
    }
    default {
        Write-Error "Invalid action: $Action"
        Write-Info "Valid actions: start, stop, restart, status, logs, cleanup, build"
        Write-Info "Usage examples:"
        Write-Info "  .\docker-deploy.ps1 -Action start"
        Write-Info "  .\docker-deploy.ps1 -Action logs -Service airflow-webserver"
        Write-Info "  .\docker-deploy.ps1 -Action stop"
    }
}

Write-Host "`n" -NoNewline