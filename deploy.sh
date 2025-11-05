#!/bin/bash

# MetaLayer Production Startup Script
# This script helps deploy the MetaLayer project safely in production

set -euo pipefail  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 MetaLayer Production Deployment Script${NC}"
echo "=================================================="

# Function to log messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose is not installed. Please install docker-compose first."
        exit 1
    fi
    
    log_info "✅ Prerequisites check passed"
}

# Function to validate environment configuration
validate_environment() {
    log_info "Validating environment configuration..."
    
    if [ ! -f ".env" ]; then
        log_error ".env file not found!"
        log_info "Please copy .env.template to .env and update the values:"
        log_info "cp .env.template .env"
        exit 1
    fi
    
    # Check for placeholder values that need to be changed
    PLACEHOLDERS=(
        "CHANGE_ME_SECURE_PASSWORD_HERE"
        "GENERATE_32_CHAR_SECRET_KEY_HERE" 
        "GENERATE_FERNET_KEY_HERE"
        "your-email@yourdomain.com"
    )
    
    for placeholder in "${PLACEHOLDERS[@]}"; do
        if grep -q "$placeholder" .env; then
            log_error "Found placeholder value '$placeholder' in .env file"
            log_info "Please update all placeholder values in .env before deployment"
            exit 1
        fi
    done
    
    log_info "✅ Environment configuration validated"
}

# Function to generate secure keys if needed
generate_keys() {
    log_info "Checking if secure keys need generation..."
    
    # Generate Fernet key if placeholder exists
    if grep -q "GENERATE_FERNET_KEY_HERE" .env 2>/dev/null; then
        log_info "Generating new Fernet key..."
        FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
        sed -i "s/GENERATE_FERNET_KEY_HERE/$FERNET_KEY/g" .env
        log_info "✅ Fernet key generated"
    fi
    
    # Generate secret key if placeholder exists
    if grep -q "GENERATE_32_CHAR_SECRET_KEY_HERE" .env 2>/dev/null; then
        log_info "Generating new secret key..."
        SECRET_KEY=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-32)
        sed -i "s/GENERATE_32_CHAR_SECRET_KEY_HERE/$SECRET_KEY/g" .env
        log_info "✅ Secret key generated"
    fi
}

# Function to create data directories
create_directories() {
    log_info "Creating required directories..."
    
    mkdir -p data/bronze_src/erp
    mkdir -p data/bronze_src/crm
    mkdir -p logs
    mkdir -p plugins
    mkdir -p include/sql
    
    log_info "✅ Directories created"
}

# Function to validate data sources
check_data_sources() {
    log_info "Checking data source files..."
    
    REQUIRED_FILES=(
        "data/bronze_src/erp/erp_sales.csv"
        "data/bronze_src/crm/crm_customers.csv"
    )
    
    for file in "${REQUIRED_FILES[@]}"; do
        if [ ! -f "$file" ]; then
            log_warn "Data source file not found: $file"
            log_info "Creating sample file: $file"
            
            if [[ "$file" == *"erp_sales.csv" ]]; then
                mkdir -p "$(dirname "$file")"
                cat > "$file" << EOF
sales_id,customer_id,product_code,quantity,sale_amount,sale_date
S001,C001,P001,2,150.00,2024-01-01
S002,C002,P002,1,75.50,2024-01-02
S003,C001,P003,3,225.75,2024-01-03
EOF
            elif [[ "$file" == *"crm_customers.csv" ]]; then
                mkdir -p "$(dirname "$file")"
                cat > "$file" << EOF
customer_id,first_name,last_name,email,phone,city,state,zip,dob
C001,John,Doe,john.doe@email.com,555-1234,New York,NY,10001,1985-01-15
C002,Jane,Smith,jane.smith@email.com,555-5678,Los Angeles,CA,90001,1990-05-20
C003,Bob,Johnson,bob.johnson@email.com,555-9012,Chicago,IL,60601,1988-12-10
EOF
            fi
            log_info "✅ Sample data created for: $file"
        fi
    done
}

# Function to start the services
start_services() {
    log_info "Starting MetaLayer services..."
    
    # Build and start services
    docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d --build
    
    log_info "✅ Services started"
    log_info "Airflow UI will be available at: http://localhost:8081"
    log_info "PostgreSQL is available at: localhost:5433"
}

# Function to check service health
check_health() {
    log_info "Checking service health..."
    
    # Wait for services to start
    sleep 30
    
    # Check if containers are running
    if docker-compose ps | grep -q "Up"; then
        log_info "✅ Services are running"
    else
        log_error "Some services failed to start"
        docker-compose logs --tail=50
        exit 1
    fi
    
    # Check Airflow webserver health
    for i in {1..12}; do  # Try for 2 minutes
        if curl -sf http://localhost:8081/health > /dev/null 2>&1; then
            log_info "✅ Airflow webserver is healthy"
            break
        elif [ $i -eq 12 ]; then
            log_error "Airflow webserver health check failed"
            docker-compose logs webserver
            exit 1
        else
            log_info "Waiting for Airflow webserver... ($i/12)"
            sleep 10
        fi
    done
}

# Function to show deployment summary
show_summary() {
    echo ""
    echo -e "${GREEN}🎉 MetaLayer Deployment Successful!${NC}"
    echo "============================================"
    echo -e "${BLUE}Access Information:${NC}"
    echo "• Airflow UI: http://localhost:8081"
    echo "• PostgreSQL: localhost:5433"
    echo ""
    echo -e "${BLUE}Available DAGs:${NC}"
    echo "• medallion_master_orchestrator - Main pipeline orchestrator"
    echo "• init_db_schemas_pg - Initialize database schemas"
    echo "• bronze_layer_production_load - Bronze layer data loading"
    echo "• silver_layer_production_transform - Silver layer transformations"
    echo "• gold_layer_production_analytics - Gold layer analytics"
    echo ""
    echo -e "${BLUE}Next Steps:${NC}"
    echo "1. Log in to Airflow UI at http://localhost:8081"
    echo "2. Enable and run the 'medallion_master_orchestrator' DAG"
    echo "3. Monitor the pipeline execution in the Airflow UI"
    echo ""
    echo -e "${YELLOW}Important:${NC}"
    echo "• Check the logs if any DAGs fail"
    echo "• Ensure your data source files are properly formatted"
    echo "• Review the data quality reports after pipeline completion"
}

# Main execution
main() {
    check_prerequisites
    validate_environment
    generate_keys
    create_directories
    check_data_sources
    start_services
    check_health
    show_summary
}

# Run main function
main "$@"