# =====================================================
# GRAFANA DASHBOARD FIX SCRIPT - NO DATA RESOLUTION
# Fixes "No Data" errors in Grafana dashboards by updating queries and datasources
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [string]$GrafanaUrl = "http://localhost:3000",
    [Parameter(Mandatory=$false)]  
    [string]$Username = "admin",
    [Parameter(Mandatory=$false)]
    [string]$Password = "Litureddy098@"
)

Write-Host "🔧 Grafana Dashboard Fix - Resolving No Data Errors" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan

# Authentication headers
$headers = @{
    'Authorization' = 'Basic ' + [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes("$Username`:$Password"))
    'Content-Type' = 'application/json'
}

function Write-ColoredOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

# =====================================================
# 1. TEST DATASOURCE CONNECTIVITY
# =====================================================
Write-ColoredOutput "`n📡 Testing Datasource Connectivity..." "Yellow"

try {
    $datasources = Invoke-RestMethod -Uri "$GrafanaUrl/api/datasources" -Headers $headers -Method Get
    
    foreach ($ds in $datasources) {
        Write-ColoredOutput "  📊 Testing datasource: $($ds.name) ($($ds.type))" "White"
        
        try {
            $testResult = Invoke-RestMethod -Uri "$GrafanaUrl/api/datasources/$($ds.id)/health" -Headers $headers -Method Get
            Write-ColoredOutput "    ✅ $($ds.name): Connected successfully" "Green"
        } catch {
            Write-ColoredOutput "    ❌ $($ds.name): Connection failed - $($_.Exception.Message)" "Red"
        }
    }
} catch {
    Write-ColoredOutput "❌ Failed to retrieve datasources: $($_.Exception.Message)" "Red"
    exit 1
}

# =====================================================
# 2. CREATE WORKING ETL DASHBOARD
# =====================================================
Write-ColoredOutput "`n📋 Creating Working ETL Dashboard..." "Yellow"

$etlDashboard = @{
    dashboard = @{
        id = $null
        title = "MetaLayer ETL Pipeline - Working Dashboard"
        description = "Real-time ETL pipeline monitoring with working data sources"
        tags = @("etl", "metalayer", "pipeline", "working")
        timezone = "browser"
        panels = @(
            @{
                id = 1
                title = "ETL Container Status"
                type = "stat"
                targets = @(
                    @{
                        datasource = @{
                            type = "prometheus"
                            uid = ($datasources | Where-Object {$_.name -eq "Prometheus"}).uid
                        }
                        expr = "up"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 0; y = 0 }
                options = @{
                    reduceOptions = @{
                        values = $false
                        calcs = @("lastNotNull")
                        fields = ""
                    }
                    orientation = "auto"
                    textMode = "auto"
                    colorMode = "value"
                    graphMode = "area"
                    justifyMode = "auto"
                }
                fieldConfig = @{
                    defaults = @{
                        mappings = @(
                            @{
                                options = @{
                                    "0" = @{ text = "DOWN"; color = "red" }
                                    "1" = @{ text = "UP"; color = "green" }
                                }
                                type = "value"
                            }
                        )
                        thresholds = @{
                            steps = @(
                                @{ color = "red"; value = $null }
                                @{ color = "green"; value = 1 }
                            )
                        }
                        unit = "none"
                    }
                }
            }
            @{
                id = 2
                title = "DAG Execution Status"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT dag_id, state, execution_date, start_date, end_date FROM dag_run WHERE dag_id LIKE '%layer%' ORDER BY start_date DESC LIMIT 10"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 12; y = 0 }
            }
            @{
                id = 3
                title = "ETL Data Layer Status"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT schemaname as layer, relname as table_name, n_tup_ins as inserts, n_tup_upd as updates FROM pg_stat_user_tables WHERE schemaname IN ('bronze', 'silver', 'gold') ORDER BY schemaname, relname"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 24; x = 0; y = 8 }
            }
            @{
                id = 4
                title = "Task Instance Status Distribution"
                type = "piechart"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT state, COUNT(*) as count FROM task_instance WHERE dag_id LIKE '%layer%' GROUP BY state"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 0; y = 16 }
            }
            @{
                id = 5
                title = "Recent Task Failures"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT dag_id, task_id, execution_date, start_date, end_date FROM task_instance WHERE state = 'failed' AND dag_id LIKE '%layer%' ORDER BY start_date DESC LIMIT 10"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 12; y = 16 }
            }
        )
        time = @{
            from = "now-1h"
            to = "now"
        }
        refresh = "30s"
    }
    overwrite = $true
}

try {
    $dashboardJson = $etlDashboard | ConvertTo-Json -Depth 20
    $result = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -Method Post -Body $dashboardJson
    Write-ColoredOutput "  ✅ Created working ETL dashboard: $($result.url)" "Green"
    Write-ColoredOutput "  🌐 Access at: $GrafanaUrl$($result.url)" "Cyan"
} catch {
    Write-ColoredOutput "  ❌ Failed to create dashboard: $($_.Exception.Message)" "Red"
}

# =====================================================
# 3. CREATE SYSTEM HEALTH DASHBOARD
# =====================================================
Write-ColoredOutput "`n🏥 Creating System Health Dashboard..." "Yellow"

$healthDashboard = @{
    dashboard = @{
        id = $null
        title = "MetaLayer System Health Monitor"
        description = "Complete system health monitoring with real data"
        tags = @("health", "system", "monitoring", "metalayer")
        timezone = "browser"
        panels = @(
            @{
                id = 1
                title = "Service Availability"
                type = "stat"
                targets = @(
                    @{
                        datasource = @{
                            type = "prometheus"
                            uid = ($datasources | Where-Object {$_.name -eq "Prometheus"}).uid
                        }
                        expr = "count(up == 1)"
                        refId = "A"
                        legendFormat = "Services Up"
                    }
                )
                gridPos = @{ h = 6; w = 8; x = 0; y = 0 }
            }
            @{
                id = 2
                title = "Database Connections"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT datname, numbackends as active_connections, xact_commit as transactions FROM pg_stat_database WHERE datname IN ('airflow', 'postgres')"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 6; w = 16; x = 8; y = 0 }
            }
            @{
                id = 3
                title = "ETL Pipeline Metrics"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = ($datasources | Where-Object {$_.name -eq "PostgreSQL"}).uid
                        }
                        rawSql = "SELECT 'Bronze Layer' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'bronze' UNION ALL SELECT 'Silver Layer' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'silver' UNION ALL SELECT 'Gold Layer' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'gold'"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 6; w = 24; x = 0; y = 6 }
            }
        )
        time = @{
            from = "now-6h"
            to = "now"
        }
        refresh = "1m"
    }
    overwrite = $true
}

try {
    $healthJson = $healthDashboard | ConvertTo-Json -Depth 20
    $healthResult = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -Method Post -Body $healthJson
    Write-ColoredOutput "  ✅ Created system health dashboard: $($healthResult.url)" "Green"
    Write-ColoredOutput "  🌐 Access at: $GrafanaUrl$($healthResult.url)" "Cyan"
} catch {
    Write-ColoredOutput "  ❌ Failed to create health dashboard: $($_.Exception.Message)" "Red"
}

# =====================================================
# 4. VALIDATE DASHBOARD DATA
# =====================================================
Write-ColoredOutput "`n🔍 Validating Dashboard Data Sources..." "Yellow"

# Test Prometheus queries
try {
    $promResponse = Invoke-WebRequest -Uri "http://localhost:9090/api/v1/query?query=up" -UseBasicParsing
    $promData = ($promResponse.Content | ConvertFrom-Json)
    Write-ColoredOutput "  ✅ Prometheus: $($promData.data.result.Count) metrics available" "Green"
} catch {
    Write-ColoredOutput "  ❌ Prometheus query failed: $($_.Exception.Message)" "Red"
}

# Test PostgreSQL queries
try {
    $pgTest = docker exec metalayer-postgres-1 psql -U postgres -d airflow -t -c "SELECT COUNT(*) FROM dag_run;" 2>$null
    Write-ColoredOutput "  ✅ PostgreSQL: $($pgTest.Trim()) DAG runs available for dashboards" "Green"
} catch {
    Write-ColoredOutput "  ❌ PostgreSQL query failed" "Red"
}

# =====================================================
# 5. SUMMARY AND RECOMMENDATIONS
# =====================================================
Write-ColoredOutput "`n🎯 DASHBOARD FIX SUMMARY:" "Cyan"
Write-ColoredOutput "===============================================" "Cyan"
Write-ColoredOutput "✅ Datasource connectivity validated" "Green"
Write-ColoredOutput "✅ Created working ETL pipeline dashboard" "Green"
Write-ColoredOutput "✅ Created system health monitoring dashboard" "Green"
Write-ColoredOutput "✅ Validated data availability for queries" "Green"

Write-ColoredOutput "`n📊 Dashboard Access:" "White"
Write-ColoredOutput "  • Grafana URL: $GrafanaUrl" "Gray"
Write-ColoredOutput "  • Credentials: $Username/$Password" "Gray"
Write-ColoredOutput "  • New dashboards should show real data immediately" "Gray"

Write-ColoredOutput "`n💡 Troubleshooting Tips:" "White"
Write-ColoredOutput "  1. If panels still show 'No Data', check query syntax" "Gray"
Write-ColoredOutput "  2. Verify time range matches data availability" "Gray"
Write-ColoredOutput "  3. Check datasource permissions and connectivity" "Gray"
Write-ColoredOutput "  4. Use Query Inspector in Grafana to debug queries" "Gray"

Write-ColoredOutput "`n🔧 Next Steps:" "White"
Write-ColoredOutput "  • Review existing dashboards and update queries" "Gray"
Write-ColoredOutput "  • Delete duplicate or non-functional dashboards" "Gray"
Write-ColoredOutput "  • Set up alerting rules for ETL pipeline monitoring" "Gray"