# MetaLayer Grafana Management Script
# Ensures reliable startup and prevents dashboard duplication issues

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "restart", "health", "logs", "clean")]
    [string]$Action
)

function Write-ColoredOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

switch ($Action) {
    "start" {
        Write-ColoredOutput "🚀 Starting Grafana with clean configuration..." "Green"
        
        # Ensure Grafana is stopped first
        docker stop metalayer-grafana 2>$null
        Start-Sleep -Seconds 2
        
        # Start with monitoring compose
        docker-compose -f docker-compose-monitoring.yml up -d grafana
        
        Write-ColoredOutput "⏳ Waiting for Grafana to be ready..." "Yellow"
        Start-Sleep -Seconds 15
        
        # Health check
        $healthCheck = docker exec metalayer-grafana wget --no-verbose --tries=1 --spider http://localhost:3000/api/health 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-ColoredOutput "✅ Grafana is healthy and ready!" "Green"
            Write-ColoredOutput "🌐 Access: http://localhost:3000 (admin/admin)" "Cyan"
        } else {
            Write-ColoredOutput "❌ Grafana health check failed" "Red"
            & $PSCommandPath -Action logs
        }
    }
    
    "restart" {
        Write-ColoredOutput "🔄 Restarting Grafana..." "Yellow"
        
        docker restart metalayer-grafana
        Start-Sleep -Seconds 15
        
        & $PSCommandPath -Action health
    }
    
    "health" {
        Write-ColoredOutput "🔍 Checking Grafana health..." "Cyan"
        
        $status = docker ps --filter "name=metalayer-grafana" --format "{{.Status}}"
        Write-ColoredOutput "Container Status: $status" "White"
        
        if ($status -like "*Up*") {
            $healthCheck = docker exec metalayer-grafana wget --no-verbose --tries=1 --spider http://localhost:3000/api/health 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-ColoredOutput "✅ Grafana is healthy!" "Green"
            } else {
                Write-ColoredOutput "⚠️  Grafana container is up but not responding" "Yellow"
            }
        } else {
            Write-ColoredOutput "❌ Grafana container is not running" "Red"
        }
    }
    
    "logs" {
        Write-ColoredOutput "📋 Grafana logs (last 20 lines):" "Cyan"
        docker logs metalayer-grafana --tail 20
    }
    
    "clean" {
        Write-ColoredOutput "🧹 Cleaning Grafana configuration..." "Yellow"
        
        # Stop Grafana
        docker stop metalayer-grafana 2>$null
        
        # Remove container (keeps volume data)
        docker rm metalayer-grafana 2>$null
        
        Write-ColoredOutput "✅ Grafana cleaned. Run 'start' to restart with clean config." "Green"
    }
}

Write-ColoredOutput "`n💡 Usage: .\grafana-manager.ps1 -Action [start|restart|health|logs|clean]" "Gray"