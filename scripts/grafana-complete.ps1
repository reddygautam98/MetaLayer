# Complete MetaLayer Grafana Setup and Recovery Script
# Handles Grafana startup, user creation, and datasource configuration

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("setup", "restart", "recover", "health", "datasources")]
    [string]$Action
)

function Write-ColoredOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

function Test-GrafanaHealth {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:3000/api/health" -Method Get -TimeoutSec 5
        return $response.status -eq "ok"
    } catch {
        return $false
    }
}

function Wait-ForGrafanaReady {
    Write-ColoredOutput "⏳ Waiting for Grafana to be ready..." "Yellow"
    $maxAttempts = 60
    $attempt = 0
    
    do {
        $attempt++
        if (Test-GrafanaHealth) {
            Write-ColoredOutput "✅ Grafana is healthy and ready!" "Green"
            return $true
        }
        Start-Sleep -Seconds 2
        if ($attempt % 10 -eq 0) {
            Write-ColoredOutput "  Still waiting... ($attempt/$maxAttempts)" "Gray"
        }
    } while ($attempt -lt $maxAttempts)
    
    Write-ColoredOutput "❌ Grafana failed to become ready after $maxAttempts attempts" "Red"
    return $false
}

switch ($Action) {
    "setup" {
        Write-ColoredOutput "🚀 Complete Grafana Setup Process..." "Cyan"
        
        # Stop any existing Grafana
        Write-ColoredOutput "🛑 Stopping existing Grafana..." "Yellow"
        docker stop metalayer-grafana 2>$null
        docker rm metalayer-grafana 2>$null
        
        # Start Grafana with minimal config
        Write-ColoredOutput "🔄 Starting Grafana with clean configuration..." "Cyan"
        docker-compose -f docker-compose-monitoring.yml up -d grafana
        
        if (Wait-ForGrafanaReady) {
            Write-ColoredOutput "🌐 Grafana is accessible at: http://localhost:3000" "Green"
            Write-ColoredOutput "🔑 Login credentials: admin / admin" "Green"
            Write-ColoredOutput "💡 Run '.\scripts\grafana-complete.ps1 -Action datasources' to add data sources" "Cyan"
        } else {
            Write-ColoredOutput "❌ Grafana setup failed" "Red"
            & $PSCommandPath -Action health
        }
    }
    
    "restart" {
        Write-ColoredOutput "🔄 Restarting Grafana..." "Yellow"
        docker restart metalayer-grafana
        Wait-ForGrafanaReady
    }
    
    "recover" {
        Write-ColoredOutput "🚑 Emergency Grafana Recovery..." "Red"
        
        # Full cleanup and restart
        docker stop metalayer-grafana 2>$null
        docker rm metalayer-grafana 2>$null
        
        # Use empty datasources to prevent startup issues
        Copy-Item "c:\Users\reddy\Downloads\MetaLayer\config\grafana\provisioning\datasources\datasources-empty.yml" "c:\Users\reddy\Downloads\MetaLayer\config\grafana\provisioning\datasources\datasources.yml" -Force
        
        # Restart with clean slate
        docker-compose -f docker-compose-monitoring.yml up -d grafana
        
        if (Wait-ForGrafanaReady) {
            Write-ColoredOutput "✅ Grafana recovery successful!" "Green"
            Write-ColoredOutput "🌐 Access: http://localhost:3000 (admin/admin)" "Cyan"
        } else {
            Write-ColoredOutput "❌ Recovery failed - check logs" "Red"
            docker logs metalayer-grafana --tail 20
        }
    }
    
    "health" {
        Write-ColoredOutput "🔍 Grafana Health Check..." "Cyan"
        
        $status = docker ps --filter "name=metalayer-grafana" --format "{{.Status}}"
        Write-ColoredOutput "Container Status: $status" "White"
        
        if (Test-GrafanaHealth) {
            Write-ColoredOutput "✅ Grafana is healthy and responsive" "Green"
            Write-ColoredOutput "🌐 Web UI: http://localhost:3000" "Cyan"
        } else {
            Write-ColoredOutput "❌ Grafana is not responding" "Red"
            Write-ColoredOutput "📋 Recent logs:" "Gray"
            docker logs metalayer-grafana --tail 10
        }
    }
    
    "datasources" {
        Write-ColoredOutput "📊 Manual Datasource Setup Instructions..." "Cyan"
        
        if (Test-GrafanaHealth) {
            Write-ColoredOutput "✅ Grafana is running" "Green"
            Write-ColoredOutput "📝 To add datasources manually:" "White"
            Write-ColoredOutput "  1. Go to http://localhost:3000" "Gray"
            Write-ColoredOutput "  2. Login with admin/admin" "Gray"  
            Write-ColoredOutput "  3. Go to Configuration > Data sources" "Gray"
            Write-ColoredOutput "  4. Add Prometheus: http://metalayer-prometheus:9090" "Gray"
            Write-ColoredOutput "  5. Add PostgreSQL: metalayer-postgres-1:5432" "Gray"
            Write-ColoredOutput "     - Database: postgres" "Gray"
            Write-ColoredOutput "     - User: postgres" "Gray"
            Write-ColoredOutput "     - Password: postgres" "Gray"
        } else {
            Write-ColoredOutput "❌ Grafana is not running. Use 'setup' or 'recover' first." "Red"
        }
    }
}

Write-ColoredOutput "`n💡 Usage: .\grafana-complete.ps1 -Action [setup|restart|recover|health|datasources]" "Gray"