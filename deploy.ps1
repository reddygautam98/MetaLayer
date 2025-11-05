# MetaLayer Production Deployment Script for Windows
# This script helps deploy the MetaLayer project safely in production

param(
    [switch]$SkipValidation = $false,
    [switch]$Force = $false
)

# Set error action preference
$ErrorActionPreference = "Stop"

# Color functions for better output
function Write-ColorOutput {
    param(
        [string]$ForegroundColor = "White",
        [string]$Message
    )
    
    $currentColor = $Host.UI.RawUI.ForegroundColor
    $Host.UI.RawUI.ForegroundColor = $ForegroundColor
    Write-Output $Message
    $Host.UI.RawUI.ForegroundColor = $currentColor
}

function Write-Info {
    param([string]$Message)
    Write-ColorOutput -ForegroundColor "Green" -Message "[INFO] $Message"
}

function Write-Warn {
    param([string]$Message)
    Write-ColorOutput -ForegroundColor "Yellow" -Message "[WARN] $Message"
}

function Write-Error {
    param([string]$Message)
    Write-ColorOutput -ForegroundColor "Red" -Message "[ERROR] $Message"
}

function Write-Header {
    param([string]$Message)
    Write-ColorOutput -ForegroundColor "Blue" -Message $Message
}

# Main deployment function
function Start-MetaLayerDeployment {
    Write-Header "🚀 MetaLayer Production Deployment Script"
    Write-Header "=================================================="

    try {
        # Step 1: Check prerequisites
        Test-Prerequisites
        
        # Step 2: Validate environment configuration
        if (-not $SkipValidation) {
            Test-EnvironmentConfiguration
        }
        
        # Step 3: Create required directories
        New-RequiredDirectories
        
        # Step 4: Check and create sample data sources
        Test-DataSources
        
        # Step 5: Start services
        Start-MetaLayerServices
        
        # Step 6: Check service health
        Test-ServiceHealth
        
        # Step 7: Show deployment summary
        Show-DeploymentSummary
        
        Write-Info "✅ MetaLayer deployment completed successfully!"
        
    }
    catch {
        Write-Error "Deployment failed: $($_.Exception.Message)"
        Write-Error "Please check the error details and try again."
        exit 1
    }
}

function Test-Prerequisites {
    Write-Info "Checking prerequisites..."
    
    # Check if Docker Desktop is installed and running
    try {
        $dockerVersion = docker --version
        Write-Info "Docker found: $dockerVersion"
    }
    catch {
        Write-Error "Docker is not installed or not in PATH. Please install Docker Desktop first."
        throw
    }
    
    # Check if Docker is running
    try {
        docker info | Out-Null
        Write-Info "Docker is running"
    }
    catch {
        Write-Error "Docker is not running. Please start Docker Desktop first."
        throw
    }
    
    # Check if docker-compose is available
    try {
        $composeVersion = docker-compose --version
        Write-Info "Docker Compose found: $composeVersion"
    }
    catch {
        Write-Error "docker-compose is not available. Please ensure Docker Desktop includes Compose."
        throw
    }
    
    Write-Info "✅ Prerequisites check passed"
}

function Test-EnvironmentConfiguration {
    Write-Info "Validating environment configuration..."
    
    if (-not (Test-Path ".env")) {
        Write-Error ".env file not found!"
        Write-Info "Please copy .env.template to .env and update the values:"
        Write-Info "Copy-Item .env.template .env"
        throw "Missing .env file"
    }
    
    # Check for placeholder values that need to be changed
    $placeholders = @(
        "CHANGE_ME_SECURE_PASSWORD_HERE",
        "GENERATE_32_CHAR_SECRET_KEY_HERE", 
        "GENERATE_FERNET_KEY_HERE",
        "your-email@yourdomain.com"
    )
    
    $envContent = Get-Content ".env" -Raw
    
    foreach ($placeholder in $placeholders) {
        if ($envContent -match [regex]::Escape($placeholder)) {
            Write-Error "Found placeholder value '$placeholder' in .env file"
            Write-Info "Please update all placeholder values in .env before deployment"
            throw "Placeholder values found in .env"
        }
    }
    
    Write-Info "✅ Environment configuration validated"
}

function New-RequiredDirectories {
    Write-Info "Creating required directories..."
    
    $directories = @(
        "data\bronze_src\erp",
        "data\bronze_src\crm", 
        "logs",
        "plugins",
        "include\sql"
    )
    
    foreach ($dir in $directories) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
            Write-Info "Created directory: $dir"
        }
    }
    
    Write-Info "✅ Directories created"
}

function Test-DataSources {
    Write-Info "Checking data source files..."
    
    $requiredFiles = @{
        "data\bronze_src\erp\erp_sales.csv" = @"
sales_id,customer_id,product_code,quantity,sale_amount,sale_date
S001,C001,P001,2,150.00,2024-01-01
S002,C002,P002,1,75.50,2024-01-02
S003,C001,P003,3,225.75,2024-01-03
S004,C003,P001,1,75.00,2024-01-04
S005,C002,P004,5,375.25,2024-01-05
"@
        "data\bronze_src\crm\crm_customers.csv" = @"
customer_id,first_name,last_name,email,phone,city,state,zip,dob
C001,John,Doe,john.doe@email.com,555-1234,New York,NY,10001,1985-01-15
C002,Jane,Smith,jane.smith@email.com,555-5678,Los Angeles,CA,90001,1990-05-20
C003,Bob,Johnson,bob.johnson@email.com,555-9012,Chicago,IL,60601,1988-12-10
C004,Alice,Brown,alice.brown@email.com,555-3456,Houston,TX,77001,1992-08-25
C005,Charlie,Wilson,charlie.wilson@email.com,555-7890,Phoenix,AZ,85001,1987-03-18
"@
    }
    
    foreach ($file in $requiredFiles.Keys) {
        if (-not (Test-Path $file)) {
            Write-Warn "Data source file not found: $file"
            Write-Info "Creating sample file: $file"
            
            # Ensure directory exists
            $directory = Split-Path $file -Parent
            if (-not (Test-Path $directory)) {
                New-Item -ItemType Directory -Path $directory -Force | Out-Null
            }
            
            # Create sample file
            $requiredFiles[$file] | Set-Content -Path $file -Encoding UTF8
            Write-Info "✅ Sample data created for: $file"
        }
        else {
            Write-Info "Found existing data source: $file"
        }
    }
}

function Start-MetaLayerServices {
    Write-Info "Starting MetaLayer services..."
    Write-Info "This may take a few minutes as Docker images are built and containers started..."
    
    try {
        # Stop any existing containers
        docker-compose down 2>$null
        
        # Build and start services
        docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d --build
        
        Write-Info "✅ Services started"
        Write-Info "Airflow UI will be available at: http://localhost:8081"
        Write-Info "PostgreSQL is available at: localhost:5433"
    }
    catch {
        Write-Error "Failed to start services: $($_.Exception.Message)"
        Write-Info "Checking container logs..."
        docker-compose logs --tail=50
        throw
    }
}

function Test-ServiceHealth {
    Write-Info "Checking service health..."
    
    # Wait for services to start
    Write-Info "Waiting for services to initialize..."
    Start-Sleep -Seconds 30
    
    # Check if containers are running
    $runningContainers = docker-compose ps --services --filter "status=running"
    if ($runningContainers) {
        Write-Info "✅ Services are running"
    }
    else {
        Write-Error "Some services failed to start"
        docker-compose logs --tail=50
        throw "Service startup failed"
    }
    
    # Check Airflow webserver health
    Write-Info "Waiting for Airflow webserver to become ready..."
    $maxAttempts = 12  # 2 minutes with 10-second intervals
    
    for ($i = 1; $i -le $maxAttempts; $i++) {
        try {
            $response = Invoke-WebRequest -Uri "http://localhost:8081/health" -Method GET -TimeoutSec 5 -ErrorAction Stop
            if ($response.StatusCode -eq 200) {
                Write-Info "✅ Airflow webserver is healthy"
                break
            }
        }
        catch {
            if ($i -eq $maxAttempts) {
                Write-Error "Airflow webserver health check failed after $maxAttempts attempts"
                docker-compose logs webserver
                throw "Webserver health check failed"
            }
            else {
                Write-Info "Waiting for Airflow webserver... ($i/$maxAttempts)"
                Start-Sleep -Seconds 10
            }
        }
    }
}

function Show-DeploymentSummary {
    Write-Output ""
    Write-Header "🎉 MetaLayer Deployment Successful!"
    Write-Header "============================================"
    Write-Header "Access Information:"
    Write-Output "• Airflow UI: http://localhost:8081"
    Write-Output "• PostgreSQL: localhost:5433"
    Write-Output ""
    Write-Header "Available DAGs:"
    Write-Output "• medallion_master_orchestrator - Main pipeline orchestrator"
    Write-Output "• init_db_schemas_pg - Initialize database schemas"  
    Write-Output "• bronze_layer_production_load - Bronze layer data loading"
    Write-Output "• silver_layer_production_transform - Silver layer transformations"
    Write-Output "• gold_layer_production_analytics - Gold layer analytics"
    Write-Output ""
    Write-Header "Next Steps:"
    Write-Output "1. Open your web browser and go to http://localhost:8081"
    Write-Output "2. Enable and run the 'medallion_master_orchestrator' DAG"
    Write-Output "3. Monitor the pipeline execution in the Airflow UI"
    Write-Output ""
    Write-Warn "Important Notes:"
    Write-Output "• Check the logs if any DAGs fail"
    Write-Output "• Ensure your data source files are properly formatted CSV"
    Write-Output "• Review the data quality reports after pipeline completion"
    Write-Output "• The default login is admin/admin (change in production)"
}

# Execute the main deployment function
Start-MetaLayerDeployment