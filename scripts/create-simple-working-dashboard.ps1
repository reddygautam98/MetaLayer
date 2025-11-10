# =====================================================
# SIMPLE GRAFANA DASHBOARD CREATOR - BASIC WORKING VERSION
# Creates a simple dashboard with working queries to test connectivity
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [string]$GrafanaUrl = "http://localhost:3000",
    [Parameter(Mandatory=$false)]  
    [string]$Username = "admin",
    [Parameter(Mandatory=$false)]
    [string]$Password = "Litureddy098@"
)

Write-Host "🔧 Creating Simple Working Dashboard" -ForegroundColor Cyan
Write-Host "====================================" -ForegroundColor Cyan

$headers = @{
    'Authorization' = 'Basic ' + [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$Username`:$Password"))
    'Content-Type' = 'application/json'
}

# Get datasource UIDs
$datasources = Invoke-RestMethod -Uri "$GrafanaUrl/api/datasources" -Headers $headers -Method Get
$promDs = $datasources | Where-Object {$_.name -eq "Prometheus"}

Write-Host "📊 Creating simple Prometheus-only dashboard..." -ForegroundColor Yellow

# Create a simple dashboard with only Prometheus data (which we know works)
$simpleDashboard = @{
    dashboard = @{
        id = $null
        uid = "simple-working-etl"
        title = "Simple ETL Monitor - Working"
        description = "Basic working dashboard with Prometheus data"
        tags = @("simple", "working", "etl")
        timezone = "browser"
        refresh = "30s"
        time = @{
            from = "now-1h"
            to = "now"
        }
        panels = @(
            @{
                id = 1
                title = "Service Status"
                type = "stat"
                targets = @(
                    @{
                        datasource = @{
                            type = "prometheus"
                            uid = $promDs.uid
                        }
                        expr = "up"
                        refId = "A"
                        legendFormat = "{{job}}"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 0; y = 0 }
                options = @{
                    reduceOptions = @{
                        values = $false
                        calcs = @("lastNotNull")
                    }
                    textMode = "value_and_name"
                    colorMode = "background"
                }
                fieldConfig = @{
                    defaults = @{
                        thresholds = @{
                            steps = @(
                                @{ color = "red"; value = 0 }
                                @{ color = "green"; value = 1 }
                            )
                        }
                        mappings = @(
                            @{
                                options = @{
                                    "0" = @{ text = "DOWN"; color = "red" }
                                    "1" = @{ text = "UP"; color = "green" }
                                }
                                type = "value"
                            }
                        )
                    }
                }
            }
            @{
                id = 2
                title = "Total Services"
                type = "stat"
                targets = @(
                    @{
                        datasource = @{
                            type = "prometheus"
                            uid = $promDs.uid
                        }
                        expr = "count(up)"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 4; w = 6; x = 12; y = 0 }
                fieldConfig = @{
                    defaults = @{
                        color = @{ mode = "palette-classic" }
                        thresholds = @{
                            steps = @(
                                @{ color = "green"; value = 0 }
                                @{ color = "yellow"; value = 5 }
                                @{ color = "green"; value = 8 }
                            )
                        }
                    }
                }
            }
            @{
                id = 3
                title = "Services UP"
                type = "stat"
                targets = @(
                    @{
                        datasource = @{
                            type = "prometheus"
                            uid = $promDs.uid
                        }
                        expr = "count(up == 1)"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 4; w = 6; x = 18; y = 0 }
                fieldConfig = @{
                    defaults = @{
                        color = @{ mode = "palette-classic" }
                        thresholds = @{
                            steps = @(
                                @{ color = "red"; value = 0 }
                                @{ color = "yellow"; value = 2 }
                                @{ color = "green"; value = 5 }
                            )
                        }
                    }
                }
            }
        )
    }
    overwrite = $true
}

try {
    $dashboardJson = $simpleDashboard | ConvertTo-Json -Depth 20
    $result = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -Method Post -Body $dashboardJson
    
    Write-Host "✅ Simple dashboard created successfully!" -ForegroundColor Green
    Write-Host "🌐 URL: $GrafanaUrl$($result.url)" -ForegroundColor Cyan
    
    Write-Host "`n📊 This dashboard shows:" -ForegroundColor White
    Write-Host "  • Individual service status (UP/DOWN)" -ForegroundColor Gray
    Write-Host "  • Total number of services monitored" -ForegroundColor Gray  
    Write-Host "  • Count of services currently UP" -ForegroundColor Gray
    Write-Host "`n💡 This uses only Prometheus data which we know works!" -ForegroundColor Yellow
    
} catch {
    Write-Host "❌ Failed to create simple dashboard: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "`n🔍 Let's also test manual PostgreSQL query..." -ForegroundColor Yellow
try {
    $testQuery = docker exec metalayer-postgres-1 psql -U postgres -d airflow -t -c "SELECT COUNT(*) as dag_count FROM dag_run;" 2>$null
    Write-Host "✅ PostgreSQL direct query works: $($testQuery.Trim()) DAG runs found" -ForegroundColor Green
    
    $testQuery2 = docker exec metalayer-postgres-1 psql -U postgres -d airflow -t -c "SELECT COUNT(*) as table_count FROM pg_tables WHERE schemaname = 'bronze';" 2>$null  
    Write-Host "✅ Bronze layer query works: $($testQuery2.Trim()) tables in bronze schema" -ForegroundColor Green
} catch {
    Write-Host "❌ PostgreSQL direct query failed" -ForegroundColor Red
}

Write-Host "`n🎯 Next Steps:" -ForegroundColor Cyan
Write-Host "1. Check the simple dashboard - it should work with Prometheus data" -ForegroundColor Gray
Write-Host "2. We'll fix PostgreSQL connectivity separately" -ForegroundColor Gray
Write-Host "3. Then add PostgreSQL panels back to working dashboards" -ForegroundColor Gray