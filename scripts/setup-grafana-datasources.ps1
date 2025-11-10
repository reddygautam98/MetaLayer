# MetaLayer Grafana Datasource Setup Script
# Adds datasources to Grafana after it's fully started to avoid startup conflicts

param(
    [Parameter(Mandatory=$false)]
    [string]$GrafanaUrl = "http://localhost:3000",
    [Parameter(Mandatory=$false)]  
    [string]$Username = "admin",
    [Parameter(Mandatory=$false)]
    [string]$Password = "Litureddy098@"
)

function Write-ColoredOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

function Add-GrafanaDatasource {
    param(
        [string]$Name,
        [string]$Type,
        [string]$Url,
        [string]$Uid,
        [hashtable]$JsonData = @{},
        [hashtable]$SecureJsonData = @{},
        [bool]$IsDefault = $false
    )
    
    $datasource = @{
        name = $Name
        type = $Type
        url = $Url
        uid = $Uid
        access = "proxy"
        isDefault = $IsDefault
        jsonData = $JsonData
        secureJsonData = $SecureJsonData
    }
    
    $body = $datasource | ConvertTo-Json -Depth 5
    $headers = @{
        "Content-Type" = "application/json"
        "Authorization" = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${Username}:${Password}"))
    }
    
    try {
        $response = Invoke-RestMethod -Uri "${GrafanaUrl}/api/datasources" -Method Post -Body $body -Headers $headers
        Write-ColoredOutput "✅ Successfully added datasource: $Name" "Green"
        return $true
    } catch {
        if ($_.Exception.Response.StatusCode -eq 409) {
            Write-ColoredOutput "⚠️  Datasource '$Name' already exists" "Yellow"
            return $true
        } else {
            Write-ColoredOutput "❌ Failed to add datasource '$Name': $($_.Exception.Message)" "Red"
            return $false
        }
    }
}

function Test-GrafanaReady {
    try {
        $response = Invoke-RestMethod -Uri "${GrafanaUrl}/api/health" -Method Get -TimeoutSec 5
        return $true
    } catch {
        return $false
    }
}

# Main execution
Write-ColoredOutput "🚀 Setting up MetaLayer Grafana datasources..." "Cyan"

# Wait for Grafana to be ready
Write-ColoredOutput "⏳ Waiting for Grafana to be ready..." "Yellow"
$maxAttempts = 30
$attempt = 0

do {
    $attempt++
    if (Test-GrafanaReady) {
        Write-ColoredOutput "✅ Grafana is ready!" "Green"
        break
    }
    Start-Sleep -Seconds 2
} while ($attempt -lt $maxAttempts)

if ($attempt -eq $maxAttempts) {
    Write-ColoredOutput "❌ Grafana is not responding after $maxAttempts attempts" "Red"
    exit 1
}

# Add Prometheus datasource
Write-ColoredOutput "🔧 Adding Prometheus datasource..." "Cyan"
$prometheusSuccess = Add-GrafanaDatasource -Name "Prometheus" -Type "prometheus" -Url "http://metalayer-prometheus:9090" -Uid "prometheus" -IsDefault $true -JsonData @{
    timeInterval = "15s"
    queryTimeout = "60s"  
    httpMethod = "GET"
}

# Add PostgreSQL datasource
Write-ColoredOutput "🔧 Adding PostgreSQL datasource..." "Cyan"
$postgresSuccess = Add-GrafanaDatasource -Name "PostgreSQL" -Type "postgres" -Url "metalayer-postgres-1:5432" -Uid "postgres" -JsonData @{
    database = "postgres"
    user = "postgres"
    sslmode = "disable"
    postgresVersion = 1500
    timescaledb = $false
} -SecureJsonData @{
    password = "postgres"
}

# Summary
Write-ColoredOutput "`n📊 Datasource Setup Summary:" "White"
if ($prometheusSuccess) {
    Write-ColoredOutput "  ✅ Prometheus: Ready for metrics monitoring" "Green"
} else {
    Write-ColoredOutput "  ❌ Prometheus: Failed to configure" "Red"
}

if ($postgresSuccess) {
    Write-ColoredOutput "  ✅ PostgreSQL: Ready for database queries" "Green"
} else {
    Write-ColoredOutput "  ❌ PostgreSQL: Failed to configure" "Red"
}

Write-ColoredOutput "`n🌐 Access Grafana: ${GrafanaUrl} (${Username}/${Password})" "Cyan"
Write-ColoredOutput "📈 Your monitoring dashboards are ready to use!" "Green"