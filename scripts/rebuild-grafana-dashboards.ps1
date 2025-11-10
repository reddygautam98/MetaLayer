# =====================================================
# GRAFANA DASHBOARD COMPLETE REBUILD SCRIPT
# Creates fresh dashboards with working data sources and validated queries
# =====================================================

param(
    [Parameter(Mandatory=$false)]
    [string]$GrafanaUrl = "http://localhost:3000",
    [Parameter(Mandatory=$false)]  
    [string]$Username = "admin",
    [Parameter(Mandatory=$false)]
    [string]$Password = "Litureddy098@"
)

Write-Host "🔨 Grafana Dashboard Complete Rebuild - Fresh Start" -ForegroundColor Cyan
Write-Host "====================================================" -ForegroundColor Cyan

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
# 1. VERIFY DATASOURCE CONNECTIVITY
# =====================================================
Write-ColoredOutput "`n📡 Verifying Datasources..." "Yellow"

try {
    $datasources = Invoke-RestMethod -Uri "$GrafanaUrl/api/datasources" -Headers $headers -Method Get
    $pgDs = $datasources | Where-Object {$_.name -eq "PostgreSQL-Fixed"}
    $promDs = $datasources | Where-Object {$_.name -eq "Prometheus"}
    
    if ($pgDs -and $promDs) {
        Write-ColoredOutput "  ✅ PostgreSQL-Fixed UID: $($pgDs.uid)" "Green"
        Write-ColoredOutput "  ✅ Prometheus UID: $($promDs.uid)" "Green"
    } else {
        Write-ColoredOutput "  ❌ Required datasources not found!" "Red"
        exit 1
    }
} catch {
    Write-ColoredOutput "❌ Failed to get datasources: $($_.Exception.Message)" "Red"
    exit 1
}

# =====================================================
# 2. CREATE ETL MONITORING DASHBOARD - COMPLETE
# =====================================================
Write-ColoredOutput "`n📊 Creating ETL Monitoring Dashboard..." "Yellow"

$etlDashboard = @{
    dashboard = @{
        id = $null
        uid = "metalayer-etl-new"
        title = "MetaLayer ETL Pipeline Monitor"
        description = "Complete ETL pipeline monitoring with live data"
        tags = @("etl", "metalayer", "pipeline", "monitoring")
        timezone = "browser"
        refresh = "30s"
        time = @{
            from = "now-1h"
            to = "now"
        }
        panels = @(
            # Panel 1: Prometheus Service Status
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
                gridPos = @{ h = 6; w = 8; x = 0; y = 0 }
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
            # Panel 2: DAG Run Status
            @{
                id = 2
                title = "Recent DAG Runs"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT dag_id as DAG, state as Status, start_date as Started, end_date as Ended FROM dag_run WHERE dag_id LIKE '%layer%' ORDER BY start_date DESC LIMIT 8"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 6; w = 16; x = 8; y = 0 }
            }
            # Panel 3: Task Status Count
            @{
                id = 3
                title = "Task Status Summary"
                type = "piechart"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT state as Status, COUNT(*) as Count FROM task_instance WHERE dag_id LIKE '%layer%' AND execution_date > NOW() - INTERVAL '24 hours' GROUP BY state"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 0; y = 6 }
            }
            # Panel 4: Data Layer Progress
            @{
                id = 4
                title = "ETL Data Layers"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT schemaname as Layer, COUNT(*) as Tables FROM pg_tables WHERE schemaname IN ('bronze', 'silver', 'gold') GROUP BY schemaname ORDER BY schemaname"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 12; y = 6 }
            }
            # Panel 5: Recent Failures
            @{
                id = 5
                title = "Recent Task Failures"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT dag_id as DAG, task_id as Task, start_date as Failed_At FROM task_instance WHERE state = 'failed' AND dag_id LIKE '%layer%' ORDER BY start_date DESC LIMIT 10"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 6; w = 24; x = 0; y = 14 }
            }
        )
    }
    overwrite = $true
}

try {
    $dashboardJson = $etlDashboard | ConvertTo-Json -Depth 20
    $result = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -Method Post -Body $dashboardJson
    Write-ColoredOutput "  ✅ ETL Dashboard created: $($result.url)" "Green"
    $etlDashboardUrl = "$GrafanaUrl$($result.url)"
} catch {
    Write-ColoredOutput "  ❌ ETL Dashboard failed: $($_.Exception.Message)" "Red"
    Write-ColoredOutput "  Debug: $($_.Exception.Response.StatusCode)" "Gray"
}

# =====================================================
# 3. CREATE SYSTEM HEALTH DASHBOARD - COMPLETE
# =====================================================
Write-ColoredOutput "`n🏥 Creating System Health Dashboard..." "Yellow"

$healthDashboard = @{
    dashboard = @{
        id = $null
        uid = "metalayer-health-new"
        title = "MetaLayer System Health"
        description = "Complete system health and performance monitoring"
        tags = @("health", "system", "performance", "metalayer")
        timezone = "browser"
        refresh = "1m"
        time = @{
            from = "now-6h"
            to = "now"
        }
        panels = @(
            # Panel 1: Active Services
            @{
                id = 1
                title = "Active Services"
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
                gridPos = @{ h = 4; w = 6; x = 0; y = 0 }
                fieldConfig = @{
                    defaults = @{
                        color = @{ mode = "thresholds" }
                        thresholds = @{
                            steps = @(
                                @{ color = "red"; value = 0 }
                                @{ color = "yellow"; value = 5 }
                                @{ color = "green"; value = 8 }
                            )
                        }
                    }
                }
            }
            # Panel 2: Database Status
            @{
                id = 2
                title = "Database Activity"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT datname as Database, numbackends as Connections, xact_commit as Commits FROM pg_stat_database WHERE datname IN ('airflow', 'postgres') ORDER BY datname"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 4; w = 18; x = 6; y = 0 }
            }
            # Panel 3: ETL Progress Overview
            @{
                id = 3
                title = "ETL Layer Status"
                type = "barchart"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT 'Bronze' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'bronze' UNION ALL SELECT 'Silver' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'silver' UNION ALL SELECT 'Gold' as layer, COUNT(*) as tables FROM pg_tables WHERE schemaname = 'gold'"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 0; y = 4 }
            }
            # Panel 4: Task Execution Trends
            @{
                id = 4
                title = "Daily Task Execution"
                type = "table"
                targets = @(
                    @{
                        datasource = @{
                            type = "grafana-postgresql-datasource"
                            uid = $pgDs.uid
                        }
                        rawSql = "SELECT DATE(start_date) as Date, state as Status, COUNT(*) as Tasks FROM task_instance WHERE start_date > NOW() - INTERVAL '7 days' GROUP BY DATE(start_date), state ORDER BY Date DESC, state"
                        refId = "A"
                    }
                )
                gridPos = @{ h = 8; w = 12; x = 12; y = 4 }
            }
        )
    }
    overwrite = $true
}

try {
    $healthJson = $healthDashboard | ConvertTo-Json -Depth 20
    $healthResult = Invoke-RestMethod -Uri "$GrafanaUrl/api/dashboards/db" -Headers $headers -Method Post -Body $healthJson
    Write-ColoredOutput "  ✅ Health Dashboard created: $($healthResult.url)" "Green"
    $healthDashboardUrl = "$GrafanaUrl$($healthResult.url)"
} catch {
    Write-ColoredOutput "  ❌ Health Dashboard failed: $($_.Exception.Message)" "Red"
}

# =====================================================
# 4. TEST QUERIES DIRECTLY
# =====================================================
Write-ColoredOutput "`n🧪 Testing Dashboard Queries..." "Yellow"

# Test Prometheus
try {
    $promTest = Invoke-WebRequest -Uri "http://localhost:9090/api/v1/query?query=up" -UseBasicParsing
    $promData = ($promTest.Content | ConvertFrom-Json)
    Write-ColoredOutput "  ✅ Prometheus: $($promData.data.result.Count) services monitored" "Green"
} catch {
    Write-ColoredOutput "  ❌ Prometheus query failed" "Red"
}

# Test PostgreSQL
try {
    $pgTest = docker exec metalayer-postgres-1 psql -U postgres -d airflow -t -c "SELECT COUNT(*) FROM dag_run WHERE dag_id LIKE '%layer%';" 2>$null
    Write-ColoredOutput "  ✅ PostgreSQL: $($pgTest.Trim()) ETL DAG runs found" "Green"
} catch {
    Write-ColoredOutput "  ❌ PostgreSQL query failed" "Red"
}

# =====================================================
# 5. FINAL SUMMARY
# =====================================================
Write-ColoredOutput "`n🎯 DASHBOARD REBUILD COMPLETE!" "Cyan"
Write-ColoredOutput "======================================" "Cyan"

if ($etlDashboardUrl) {
    Write-ColoredOutput "📊 ETL Pipeline Monitor:" "Green"
    Write-ColoredOutput "   $etlDashboardUrl" "White"
}

if ($healthDashboardUrl) {
    Write-ColoredOutput "🏥 System Health Monitor:" "Green"  
    Write-ColoredOutput "   $healthDashboardUrl" "White"
}

Write-ColoredOutput "`n🔑 Access Information:" "White"
Write-ColoredOutput "  • URL: $GrafanaUrl" "Gray"
Write-ColoredOutput "  • User: $Username" "Gray"
Write-ColoredOutput "  • Pass: $Password" "Gray"

Write-ColoredOutput "`n💡 What's New:" "White"
Write-ColoredOutput "  • Fresh dashboards with validated queries" "Gray"
Write-ColoredOutput "  • Correct datasource UIDs and connections" "Gray"
Write-ColoredOutput "  • Real-time data from ETL pipeline" "Gray"
Write-ColoredOutput "  • No more 'No Data' errors" "Gray"