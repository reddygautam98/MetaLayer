# MetaLayer Environment Validation Script
# Validates that all production optimizations are properly configured

param(
    [switch]$Verbose = $false
)

$ErrorActionPreference = "Stop"

function Write-ValidationLog {
    param([string]$Message, [string]$Level = "INFO")
    $color = switch ($Level) {
        "PASS" { "Green" }
        "FAIL" { "Red" }
        "WARN" { "Yellow" }
        "INFO" { "White" }
        default { "White" }
    }
    Write-Host "[$Level] $Message" -ForegroundColor $color
}

function Test-Validation {
    param(
        [string]$Name,
        [scriptblock]$Test,
        [string]$FailureMessage = "Check failed"
    )
    
    try {
        $result = & $Test
        if ($result) {
            Write-ValidationLog "✓ $Name" "PASS"
            return $true
        } else {
            Write-ValidationLog "✗ $Name - $FailureMessage" "FAIL"
            return $false
        }
    } catch {
        Write-ValidationLog "✗ $Name - Error: $($_.Exception.Message)" "FAIL"
        return $false
    }
}

Write-ValidationLog "MetaLayer Production Environment Validation" "INFO"
Write-ValidationLog "=" * 50 "INFO"

$validationsPassed = 0
$totalValidations = 0

# 1. Docker Compose Resource Limits
$totalValidations++
if (Test-Validation -Name "Docker Compose has resource limits" -Test {
    $composeContent = Get-Content "docker-compose.yml" -Raw -ErrorAction SilentlyContinue
    return $composeContent -match "deploy:" -and $composeContent -match "resources:" -and $composeContent -match "limits:"
} -FailureMessage "Resource limits not configured in docker-compose.yml") {
    $validationsPassed++
}

# 2. Enhanced Health Checks
$totalValidations++
if (Test-Validation -Name "Enhanced health checks configured" -Test {
    $composeContent = Get-Content "docker-compose.yml" -Raw -ErrorAction SilentlyContinue
    return $composeContent -match "healthcheck:" -and $composeContent -match "CMD-SHELL"
} -FailureMessage "Enhanced health checks not found") {
    $validationsPassed++
}

# 3. Monitoring Stack
$totalValidations++
if (Test-Validation -Name "Monitoring stack configured" -Test {
    $overrideContent = Get-Content "docker-compose.override.yml" -Raw -ErrorAction SilentlyContinue
    return $overrideContent -match "prometheus:" -and $overrideContent -match "grafana:"
} -FailureMessage "Monitoring stack not configured") {
    $validationsPassed++
}

# 4. Multi-stage Dockerfile
$totalValidations++
if (Test-Validation -Name "Multi-stage Dockerfile implemented" -Test {
    $dockerContent = Get-Content "Dockerfile" -Raw -ErrorAction SilentlyContinue
    return $dockerContent -match "FROM.*AS builder" -and $dockerContent -match "FROM.*AS production"
} -FailureMessage "Multi-stage build not implemented") {
    $validationsPassed++
}

# 5. PostgreSQL Configuration
$totalValidations++
if (Test-Validation -Name "PostgreSQL optimization file exists" -Test {
    return Test-Path "config\postgresql.conf"
} -FailureMessage "PostgreSQL configuration file missing") {
    $validationsPassed++
}

# 6. Prometheus Configuration
$totalValidations++
if (Test-Validation -Name "Prometheus configuration exists" -Test {
    return Test-Path "config\prometheus.yml"
} -FailureMessage "Prometheus configuration missing") {
    $validationsPassed++
}

# 7. Grafana Provisioning
$totalValidations++
if (Test-Validation -Name "Grafana provisioning configured" -Test {
    return (Test-Path "config\grafana\provisioning\datasources\prometheus.yml") -and 
           (Test-Path "config\grafana\provisioning\dashboards\dashboards.yml")
} -FailureMessage "Grafana provisioning not configured") {
    $validationsPassed++
}

# 8. Enhanced Airflow Settings
$totalValidations++
if (Test-Validation -Name "Enhanced Airflow settings configured" -Test {
    $airflowContent = Get-Content "airflow_settings.yaml" -Raw -ErrorAction SilentlyContinue
    return $airflowContent -match "PROMETHEUS_METRICS_ENABLED" -and $airflowContent -match "PARALLEL_PROCESSING_ENABLED"
} -FailureMessage "Enhanced Airflow settings not found") {
    $validationsPassed++
}

# 9. Backup Scripts
$totalValidations++
if (Test-Validation -Name "Backup scripts available" -Test {
    return (Test-Path "scripts\backup.ps1") -and (Test-Path "scripts\backup.sh")
} -FailureMessage "Backup scripts missing") {
    $validationsPassed++
}

# 10. Security Scripts
$totalValidations++
if (Test-Validation -Name "Security hardening script exists" -Test {
    return Test-Path "scripts\security_hardening.ps1"
} -FailureMessage "Security hardening script missing") {
    $validationsPassed++
}

# 11. Environment Variables
$totalValidations++
if (Test-Validation -Name "Environment template configured" -Test {
    $envContent = Get-Content ".env.template" -Raw -ErrorAction SilentlyContinue
    return $envContent -match "POSTGRES_PASSWORD" -and $envContent -match "AIRFLOW__WEBSERVER__SECRET_KEY"
} -FailureMessage "Environment template not properly configured") {
    $validationsPassed++
}

# 12. Configuration Directories
$totalValidations++
if (Test-Validation -Name "Required config directories exist" -Test {
    return (Test-Path "config") -and 
           (Test-Path "config\grafana") -and 
           (Test-Path "scripts")
} -FailureMessage "Required directories missing") {
    $validationsPassed++
}

# 13. Dashboard Configuration
$totalValidations++
if (Test-Validation -Name "Grafana dashboard configured" -Test {
    return Test-Path "config\grafana\dashboards\metalayer-overview.json"
} -FailureMessage "Grafana dashboard not configured") {
    $validationsPassed++
}

# Calculate score
$score = [math]::Round(($validationsPassed / $totalValidations) * 100, 1)

Write-ValidationLog "" "INFO"
Write-ValidationLog "Validation Summary" "INFO"
Write-ValidationLog "=" * 20 "INFO"
Write-ValidationLog "Passed: $validationsPassed/$totalValidations" "INFO"
Write-ValidationLog "Score: $score%" "INFO"

if ($score -ge 90) {
    Write-ValidationLog "🎉 Excellent! Production environment is optimally configured." "PASS"
} elseif ($score -ge 80) {
    Write-ValidationLog "✓ Good! Production environment is well configured with minor improvements needed." "PASS"
} elseif ($score -ge 70) {
    Write-ValidationLog "⚠ Fair! Production environment needs some improvements." "WARN"
} else {
    Write-ValidationLog "❌ Poor! Production environment requires significant improvements." "FAIL"
}

Write-ValidationLog "" "INFO"
Write-ValidationLog "Next Steps:" "INFO"
Write-ValidationLog "1. Address any failed validations above" "INFO"
Write-ValidationLog "2. Run security hardening: .\scripts\security_hardening.ps1" "INFO"
Write-ValidationLog "3. Test backup procedures: .\scripts\backup.ps1 -DryRun" "INFO"
Write-ValidationLog "4. Deploy with monitoring: docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d" "INFO"

if ($score -lt 80) {
    exit 1
}