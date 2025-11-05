# 🚀 MetaLayer GitHub Actions Workflows

This directory contains comprehensive GitHub Actions workflows for the MetaLayer Medallion data pipeline project, providing enterprise-grade CI/CD, testing, security, and monitoring capabilities.

## 📋 Available Workflows

### 1. 🚀 **CI/CD Pipeline** (`ci-cd-pipeline.yml`)
**Trigger:** Push to main/develop, Pull Requests
**Purpose:** Complete continuous integration and deployment pipeline

**Features:**
- ✅ **Code Quality Analysis** - Linting, formatting, and style checks
- ✅ **DAG Validation** - Airflow DAG import and structure testing
- ✅ **Security Scanning** - Dependency and container vulnerability scans
- ✅ **Docker Build & Push** - Automated container image building
- ✅ **Multi-Environment Deployment** - Dev, staging, and production deployments
- ✅ **Slack Notifications** - Team notifications for deployment status

**Environments:**
- **Development**: Auto-deploy on `develop` branch
- **Staging**: Auto-deploy on `main` branch
- **Production**: Manual trigger or `[deploy-prod]` commit message

### 2. 🧪 **Data Quality & Testing** (`data-quality-testing.yml`)
**Trigger:** Daily schedule, Manual trigger, DAG/SQL changes
**Purpose:** Comprehensive data quality validation and testing

**Features:**
- ✅ **Data Validation Tests** - Automated data quality checks
- ✅ **SQL Validation** - Syntax and logic validation for all SQL files
- ✅ **Performance Testing** - DAG execution performance analysis
- ✅ **Sample Data Generation** - Automated test data creation
- ✅ **Quality Reports** - Detailed data quality and profiling reports

### 3. 🔒 **Security Audit & Compliance** (`security-audit.yml`)
**Trigger:** Weekly schedule, Manual trigger, Security-related changes
**Purpose:** Enterprise security scanning and compliance validation

**Features:**
- ✅ **Dependency Security Audit** - Known vulnerability scanning
- ✅ **Container Security Scanning** - Docker image vulnerability analysis
- ✅ **Code Security Analysis** - Static security analysis with Bandit/Semgrep
- ✅ **Infrastructure Security** - Docker Compose and configuration scanning
- ✅ **Compliance Checks** - Security best practices validation
- ✅ **Secret Detection** - Hardcoded credentials and API key detection

### 4. 🏗️ **Infrastructure Deployment** (`infrastructure-deployment.yml`)
**Trigger:** Manual workflow dispatch
**Purpose:** Infrastructure provisioning and deployment automation

**Features:**
- ✅ **Infrastructure Validation** - Terraform and Kubernetes manifest validation
- ✅ **Multi-Environment Support** - Development, staging, production deployments
- ✅ **Docker Compose Deployment** - Local development environment
- ✅ **AWS ECS/Fargate Deployment** - Cloud staging environment
- ✅ **Kubernetes Deployment** - Production container orchestration
- ✅ **Infrastructure Destruction** - Safe environment cleanup

### 5. 📊 **Performance Monitoring** (`performance-monitoring.yml`)
**Trigger:** Every 30 minutes, Manual trigger
**Purpose:** Continuous performance monitoring and alerting

**Features:**
- ✅ **Airflow Health Monitoring** - Scheduler, webserver, and database health
- ✅ **Database Performance** - Connection monitoring and table size tracking
- ✅ **Resource Utilization** - CPU, memory, and disk usage monitoring
- ✅ **Performance Alerting** - Automated alerts for threshold violations
- ✅ **Multi-Channel Notifications** - Slack and email alert integration

### 6. 🔄 **Pipeline Health Check** (`pipeline-health-check.yml`)
**Trigger:** Every 6 hours, Manual trigger
**Purpose:** Comprehensive pipeline health assessment

**Features:**
- ✅ **Pipeline Status Monitoring** - DAG execution status tracking
- ✅ **Data Freshness Validation** - Ensures data is current and updated
- ✅ **Data Quality Assessment** - Automated quality score calculation
- ✅ **Performance Metrics** - Execution time and success rate monitoring
- ✅ **Health Summary Reports** - Comprehensive status dashboards

## 🔧 Setup Requirements

### 1. **Required GitHub Secrets**

Create these secrets in your GitHub repository settings:

```bash
# AWS Credentials (for cloud deployments)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_ACCESS_KEY_ID_PROD=your_prod_aws_access_key
AWS_SECRET_ACCESS_KEY_PROD=your_prod_aws_secret_key

# Airflow Configuration
AIRFLOW_URL=https://your-airflow-instance.com
AIRFLOW_USERNAME=admin
AIRFLOW_PASSWORD=your_secure_password

# Database Configuration
DATABASE_URL=postgresql://user:password@host:port/database

# Notification Configuration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
PERFORMANCE_SLACK_WEBHOOK=https://hooks.slack.com/services/YOUR/PERFORMANCE/URL
SECURITY_SLACK_WEBHOOK=https://hooks.slack.com/services/YOUR/SECURITY/URL
PIPELINE_HEALTH_WEBHOOK=https://hooks.slack.com/services/YOUR/HEALTH/URL

# Email Alerts
EMAIL_USERNAME=your-email@domain.com
EMAIL_PASSWORD=your_app_password
ALERT_EMAIL_RECIPIENTS=team@yourdomain.com
```

### 2. **Required GitHub Environments**

Create these environments in your repository:

1. **development** - For development deployments
2. **staging** - For staging deployments  
3. **production** - For production deployments (with protection rules)

### 3. **Repository Configuration**

Ensure your repository has:
- ✅ Actions enabled
- ✅ Write permissions for GitHub Actions
- ✅ Branch protection rules for `main` branch
- ✅ Required status checks configured

## 📊 Monitoring & Alerting

### **Performance Thresholds**
- **CPU Usage**: Alert at 80%
- **Memory Usage**: Alert at 80%
- **DAG Success Rate**: Alert below 95%
- **Data Quality Score**: Alert below 95%

### **Notification Channels**
- **Slack**: Real-time team notifications
- **Email**: Critical alerts and reports
- **GitHub**: Pull request and deployment status

### **Health Check Frequency**
- **Performance Monitoring**: Every 30 minutes
- **Pipeline Health**: Every 6 hours
- **Security Audit**: Weekly
- **Data Quality**: Daily

## 🚀 Getting Started

### 1. **Enable Workflows**
```bash
# All workflows are automatically enabled when you push to your repository
git add .github/workflows/
git commit -m "Add GitHub Actions workflows"
git push origin main
```

### 2. **Configure Secrets**
1. Go to repository **Settings → Secrets and variables → Actions**
2. Add all required secrets from the list above
3. Configure environment-specific secrets

### 3. **Set Up Environments**
1. Go to repository **Settings → Environments**
2. Create development, staging, and production environments
3. Configure protection rules for production

### 4. **First Deployment**
```bash
# Trigger manual deployment to development
# Go to Actions → Infrastructure Deployment → Run workflow
# Select "development" environment and "deploy" action
```

## 📈 Workflow Outputs

### **Artifacts Generated**
- 📊 Code quality reports
- 🧪 Test coverage reports
- 🔒 Security scan results
- 📊 Performance metrics
- 📋 Data quality reports
- 🏥 Health check summaries

### **Reports Available**
- **Daily**: Data quality and pipeline health
- **Weekly**: Security audit and compliance
- **Per Deployment**: Performance and validation reports
- **Real-time**: Monitoring alerts and notifications

## 🔄 Customization

### **Modify Schedules**
Edit the `cron` expressions in workflow files:
```yaml
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
```

### **Adjust Thresholds**
Update performance thresholds in workflow files:
```yaml
env:
  ALERT_THRESHOLD: '80'  # Change to your preferred threshold
```

### **Add Custom Checks**
Extend workflows by adding custom steps:
```yaml
- name: Custom Health Check
  run: |
    # Your custom validation logic here
    echo "Running custom checks..."
```

## 🆘 Troubleshooting

### **Common Issues**

1. **Workflow Failures**
   - Check GitHub Actions logs
   - Verify all required secrets are set
   - Ensure environment permissions are configured

2. **Deployment Issues**
   - Verify AWS credentials and permissions
   - Check Docker registry access
   - Validate Kubernetes cluster connectivity

3. **Notification Problems**
   - Test webhook URLs manually
   - Check Slack app permissions
   - Verify email SMTP settings

### **Debug Steps**
1. Check workflow run logs in GitHub Actions
2. Verify secret values (without exposing them)
3. Test individual workflow steps locally
4. Contact team for environment access issues

---

## 🎯 Next Steps

1. **Configure all required secrets** in your GitHub repository
2. **Set up environments** with appropriate protection rules
3. **Test workflows** with a sample deployment
4. **Customize thresholds** based on your requirements
5. **Monitor alerts** and adjust as needed

Your MetaLayer project now has enterprise-grade CI/CD automation! 🚀