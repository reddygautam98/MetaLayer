# =====================================================
# CUSTOM ETL HEALTH MONITOR SCRIPT
# Creates custom metrics for actual service health status
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [int]$Port = 8001
)

Write-Host "🏥 Starting Custom ETL Health Monitor on port $Port..." -ForegroundColor Cyan

# Create a simple HTTP server that exposes metrics
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("http://localhost:$Port/")
$listener.Start()

Write-Host "✅ Health monitor running at http://localhost:$Port/metrics" -ForegroundColor Green
Write-Host "📊 Monitoring ETL services..." -ForegroundColor Yellow

try {
    while ($listener.IsListening) {
        $context = $listener.GetContext()
        $request = $context.Request
        $response = $context.Response
        
        if ($request.Url.LocalPath -eq "/metrics") {
            # Get actual container health status
            $metrics = @()
            
            # Check each ETL service
            $services = @(
                @{name="prometheus"; container="metalayer-prometheus"},
                @{name="grafana"; container="metalayer-grafana"},
                @{name="postgres"; container="metalayer-postgres-1"},
                @{name="redis"; container="metalayer-redis-1"},
                @{name="airflow_webserver"; container="metalayer-webserver-1"},
                @{name="airflow_scheduler"; container="metalayer-scheduler-1"},
                @{name="airflow_worker"; container="metalayer-worker-1"},
                @{name="metrics_exporter"; container="metalayer-metrics-exporter-1"}
            )
            
            foreach ($service in $services) {
                try {
                    $status = docker inspect --format='{{.State.Status}}' $service.container 2>$null
                    $health = docker inspect --format='{{.State.Health.Status}}' $service.container 2>$null
                    
                    $value = if ($status -eq "running") { 
                        if ($health -eq "healthy" -or $health -eq "<no value>") { 1 } else { 0.5 }
                    } else { 0 }
                    
                    $metrics += "etl_service_status{service=`"$($service.name)`",container=`"$($service.container)`"} $value"
                } catch {
                    $metrics += "etl_service_status{service=`"$($service.name)`",container=`"$($service.container)`"} 0"
                }
            }
            
            # Add timestamp
            $metrics += "# TYPE etl_service_status gauge"
            $metrics += "# HELP etl_service_status ETL service health status (1=healthy, 0.5=unhealthy, 0=down)"
            
            $metricsText = ($metrics -join "`n") + "`n"
            $buffer = [System.Text.Encoding]::UTF8.GetBytes($metricsText)
            
            $response.ContentType = "text/plain"
            $response.ContentLength64 = $buffer.Length
            $response.OutputStream.Write($buffer, 0, $buffer.Length)
        } else {
            $response.StatusCode = 404
        }
        
        $response.Close()
        Start-Sleep -Milliseconds 100
    }
} catch {
    Write-Host "❌ Error: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    $listener.Stop()
    Write-Host "🛑 Health monitor stopped" -ForegroundColor Yellow
}