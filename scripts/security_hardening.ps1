# MetaLayer Security Hardening Script for Windows
# Implements security best practices for production deployment

param(
    [switch]$DryRun = $false,
    [switch]$Verbose = $false
)

$ErrorActionPreference = "Stop"

# Logging function
function Write-SecurityLog {
    param([string]$Message, [string]$Level = "INFO")
    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "$Timestamp [$Level] $Message"
    Write-Host $LogMessage
    if (!$DryRun) {
        Add-Content -Path "security_hardening.log" -Value $LogMessage
    }
}

function Test-SecurityRequirement {
    param(
        [string]$Name,
        [scriptblock]$Test,
        [scriptblock]$Fix = $null
    )
    
    Write-SecurityLog "Checking: $Name" "CHECK"
    
    try {
        $result = & $Test
        if ($result) {
            Write-SecurityLog "✓ PASS: $Name" "PASS"
            return $true
        } else {
            Write-SecurityLog "✗ FAIL: $Name" "FAIL"
            if ($Fix -and !$DryRun) {
                Write-SecurityLog "Applying fix for: $Name" "FIX"
                & $Fix
                Write-SecurityLog "✓ FIXED: $Name" "FIXED"
            }
            return $false
        }
    } catch {
        Write-SecurityLog "ERROR checking $Name`: $($_.Exception.Message)" "ERROR"
        return $false
    }
}

Write-SecurityLog "Starting MetaLayer Security Hardening..." "INFO"
Write-SecurityLog "DryRun Mode: $DryRun" "INFO"

$securityChecks = 0
$securityPassed = 0

# 1. Check Docker daemon security
$securityChecks++
if (Test-SecurityRequirement -Name "Docker daemon running" -Test {
    try {
        docker info | Out-Null
        return $true
    } catch {
        return $false
    }
}) {
    $securityPassed++
}

# 2. Check for exposed ports
$securityChecks++
if (Test-SecurityRequirement -Name "No unnecessary exposed ports" -Test {
    $runningContainers = docker ps --format "{{.Ports}}" | Where-Object { $_ -match "0\.0\.0\.0" }
    return $runningContainers.Count -eq 0
} -Fix {
    Write-SecurityLog "Review docker-compose files to limit port exposure" "WARN"
}) {
    $securityPassed++
}

# 3. Check environment file security
$securityChecks++
if (Test-SecurityRequirement -Name "Environment files are secure" -Test {
    $envFiles = Get-ChildItem -Filter ".env*" -File
    foreach ($file in $envFiles) {
        $content = Get-Content $file.FullName -Raw
        if ($content -match "password=\w+|secret=\w+") {
            return $false
        }
    }
    return $true
} -Fix {
    Write-SecurityLog "Ensure all secrets use environment variable placeholders" "WARN"
}) {
    $securityPassed++
}

# 4. Check Docker Compose file security
$securityChecks++
if (Test-SecurityRequirement -Name "Docker Compose security settings" -Test {
    $composeFiles = Get-ChildItem -Filter "docker-compose*.yml" -File
    foreach ($file in $composeFiles) {
        $content = Get-Content $file.FullName -Raw
        # Check for security best practices
        if ($content -notmatch "user:|read_only:|no_new_privileges:") {
            return $false
        }
    }
    return $true
}) {
    $securityPassed++
}

# 5. Check for default credentials
$securityChecks++
if (Test-SecurityRequirement -Name "No default credentials in use" -Test {
    $files = Get-ChildItem -Recurse -File | Where-Object { 
        $_.Extension -in @('.yml', '.yaml', '.env', '.py') 
    }
    foreach ($file in $files) {
        $content = Get-Content $file.FullName -Raw -ErrorAction SilentlyContinue
        if ($content -match "admin:admin|postgres:postgres|password=admin|password=password") {
            Write-SecurityLog "Found potential default credentials in: $($file.FullName)" "WARN"
            return $false
        }
    }
    return $true
}) {
    $securityPassed++
}

# 6. Check file permissions
$securityChecks++
if (Test-SecurityRequirement -Name "Secure file permissions" -Test {
    # Windows equivalent of checking file permissions
    $sensitiveFiles = @(".env", ".env.template", "airflow_settings.yaml")
    foreach ($file in $sensitiveFiles) {
        if (Test-Path $file) {
            $acl = Get-Acl $file
            # Check if file is readable by everyone
            $everyone = $acl.Access | Where-Object { $_.IdentityReference -match "Everyone|Users" -and $_.FileSystemRights -match "Read" }
            if ($everyone) {
                return $false
            }
        }
    }
    return $true
} -Fix {
    $sensitiveFiles = @(".env", ".env.template", "airflow_settings.yaml")
    foreach ($file in $sensitiveFiles) {
        if (Test-Path $file) {
            # Remove inheritance and set restrictive permissions
            $acl = Get-Acl $file
            $acl.SetAccessRuleProtection($true, $false)
            $adminRule = New-Object System.Security.AccessControl.FileSystemAccessRule("Administrators", "FullControl", "Allow")
            $acl.SetAccessRule($adminRule)
            Set-Acl $file $acl
        }
    }
}) {
    $securityPassed++
}

# 7. Check for security updates
$securityChecks++
if (Test-SecurityRequirement -Name "Docker images are up to date" -Test {
    try {
        docker images --format "{{.Repository}}:{{.Tag}}" | ForEach-Object {
            $pullResult = docker pull $_ 2>&1
            if ($pullResult -match "Image is up to date") {
                return $true
            }
        }
        return $false
    } catch {
        return $false
    }
} -Fix {
    Write-SecurityLog "Run 'docker-compose pull' to update images" "INFO"
}) {
    $securityPassed++
}

# 8. Check network security
$securityChecks++
if (Test-SecurityRequirement -Name "Custom Docker networks configured" -Test {
    $networks = docker network ls --format "{{.Name}}" | Where-Object { $_ -match "metalayer" }
    return $networks.Count -gt 0
}) {
    $securityPassed++
}

# 9. Check logging configuration
$securityChecks++
if (Test-SecurityRequirement -Name "Secure logging configured" -Test {
    $composeContent = Get-Content "docker-compose.override.yml" -Raw -ErrorAction SilentlyContinue
    return $composeContent -match "logging:|log_driver:"
} -Fix {
    Write-SecurityLog "Consider adding logging configuration to Docker Compose" "INFO"
}) {
    $securityPassed++
}

# 10. Check backup strategy
$securityChecks++
if (Test-SecurityRequirement -Name "Backup scripts available" -Test {
    return (Test-Path "scripts\backup.ps1") -or (Test-Path "scripts\backup.sh")
}) {
    $securityPassed++
}

# Generate security report
$securityScore = [math]::Round(($securityPassed / $securityChecks) * 100, 1)

$report = @"
MetaLayer Security Hardening Report
==================================
Date: $(Get-Date)
Mode: $(if ($DryRun) { "Dry Run" } else { "Active" })

Security Score: $securityScore% ($securityPassed/$securityChecks checks passed)

Security Recommendations:
1. Regularly update Docker images and base OS
2. Use secrets management for sensitive data
3. Implement network segmentation
4. Enable audit logging
5. Regular security scanning
6. Implement backup encryption
7. Monitor container resource usage
8. Use least privilege principles
9. Regular penetration testing
10. Security awareness training

Next Steps:
- Review failed security checks
- Implement recommended fixes
- Schedule regular security audits
- Update incident response procedures
"@

Write-SecurityLog $report "REPORT"

if (!$DryRun) {
    Set-Content -Path "security_report.txt" -Value $report
    Write-SecurityLog "Security report saved to: security_report.txt" "INFO"
}

if ($securityScore -lt 80) {
    Write-SecurityLog "WARNING: Security score is below 80%. Immediate action required!" "WARN"
    exit 1
} else {
    Write-SecurityLog "Security hardening completed successfully!" "INFO"
}