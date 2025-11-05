# GitHub Actions Deployment Guide

This document explains how to set up and use the GitHub Actions workflows for MetaLayer deployment.

## 🏗️ Workflow Overview

The MetaLayer project includes several GitHub Actions workflows for comprehensive CI/CD:

### 1. **Main CI/CD Pipeline** (`main-pipeline.yml`)
- **Triggers**: Push to main/develop branches, PRs, releases
- **Features**: Code quality, DAG validation, Docker builds, deployments
- **Environments**: Staging (develop branch), Production (main branch)

### 2. **Security & Quality Scan** (`security-quality.yml`)
- **Triggers**: Push/PR to main/develop, daily schedule, manual
- **Features**: Dependency scanning, code analysis, Docker security
- **Tools**: Bandit, Safety, Semgrep, Trivy, Hadolint

### 3. **Data Quality Testing** (`testing.yml`)
- **Triggers**: Changes to DAGs/tests, weekday schedule
- **Features**: DAG validation, data quality tests, integration tests
- **Coverage**: Unit tests, integration tests, data validation

### 4. **Production Deployment** (`production-deploy.yml`)
- **Triggers**: GitHub releases, manual workflow dispatch
- **Features**: Production-ready deployment with validation
- **Security**: Pre-deployment checks, backup creation, rollback capability

## 🔧 Setup Instructions

### 1. Repository Secrets Configuration

Add the following secrets in your GitHub repository settings:

#### Production Environment
```
PROD_POSTGRES_PASSWORD=<secure-production-db-password>
PROD_AIRFLOW_PASSWORD=<secure-airflow-admin-password>
PROD_GRAFANA_PASSWORD=<secure-grafana-admin-password>
```

#### Staging Environment
```
STAGING_POSTGRES_PASSWORD=<staging-db-password>
STAGING_AIRFLOW_PASSWORD=<staging-airflow-password>
STAGING_GRAFANA_PASSWORD=<staging-grafana-password>
```

#### Registry Access (if using private registry)
```
REGISTRY_USERNAME=<container-registry-username>
REGISTRY_PASSWORD=<container-registry-password>
```

### 2. Environment Protection Rules

Set up environment protection rules in GitHub:

#### Production Environment
- **Required reviewers**: 2+ team members
- **Deployment branches**: Only `main` branch
- **Wait timer**: 5 minutes (for last-minute cancellation)

#### Staging Environment
- **Required reviewers**: 1 team member
- **Deployment branches**: `develop` and `main` branches

### 3. Branch Protection Rules

Configure branch protection for:

#### Main Branch
- Require pull request reviews (2 reviewers)
- Require status checks (all workflows must pass)
- Require branches to be up to date
- Restrict pushes to admins only

#### Develop Branch
- Require pull request reviews (1 reviewer)
- Require status checks (quality and testing workflows)

## 📋 Deployment Workflows

### Staging Deployment

**Automatic Triggers:**
- Push to `develop` branch
- Manual workflow dispatch

**Process:**
1. Code quality and security checks
2. DAG validation with test database
3. Build and push Docker images
4. Deploy to staging environment
5. Health check verification
6. DAG loading verification

**Access Staging:**
- Airflow UI: `http://staging-server:8080`
- Grafana: `http://staging-server:3000`
- Prometheus: `http://staging-server:9090`

### Production Deployment

**Triggers:**
- GitHub release creation
- Manual workflow dispatch (with approval)

**Process:**
1. Pre-deployment validation
2. Security and environment checks
3. Production image build
4. Database and configuration backup
5. Zero-downtime deployment
6. Comprehensive health verification
7. Post-deployment monitoring setup

**Safety Features:**
- Automatic rollback on health check failure
- Database backup before deployment
- Blue-green deployment strategy
- Comprehensive monitoring and alerting

## 🔄 Development Workflow

### Feature Development
1. Create feature branch from `develop`
2. Develop and test locally
3. Create PR to `develop` branch
4. Automated quality checks run
5. Code review and approval
6. Merge triggers staging deployment

### Release Process
1. Create PR from `develop` to `main`
2. Comprehensive review and testing
3. Merge to `main` branch
4. Create GitHub release
5. Automatic production deployment

### Hotfix Process
1. Create hotfix branch from `main`
2. Implement critical fix
3. Create PR directly to `main`
4. Emergency review and approval
5. Merge triggers production deployment

## 🛡️ Security Features

### Code Security
- **Static Analysis**: Bandit for Python security issues
- **Dependency Scanning**: Safety for known vulnerabilities
- **Code Quality**: Flake8, Black, isort for consistency
- **Secret Detection**: Prevent credential commits

### Container Security
- **Image Scanning**: Trivy for container vulnerabilities
- **Dockerfile Linting**: Hadolint for best practices
- **Multi-stage Builds**: Minimal production images
- **Security Updates**: Regular base image updates

### Infrastructure Security
- **Secret Management**: GitHub Secrets for sensitive data
- **Environment Isolation**: Separate staging/production
- **Network Security**: Internal service communication
- **Access Control**: Role-based deployment approvals

## 📊 Monitoring and Alerting

### Deployment Monitoring
- Real-time health checks
- Service availability monitoring
- Performance metric collection
- Error rate tracking

### Quality Metrics
- Code coverage tracking
- Test result reporting
- Security scan results
- Performance benchmarks

### Alerting Channels
- GitHub Actions notifications
- Email alerts for failures
- Slack integration (optional)
- Grafana dashboards for metrics

## 🚨 Troubleshooting

### Common Issues

#### Deployment Failures
```bash
# Check workflow logs in GitHub Actions tab
# Verify environment secrets are set
# Ensure branch protection rules allow deployment
```

#### Health Check Failures
```bash
# Check individual service logs:
docker logs metalayer_webserver
docker logs metalayer_postgres
docker logs metalayer_grafana

# Verify network connectivity
docker network ls
docker network inspect metalayer_network
```

#### DAG Loading Issues
```bash
# Check Airflow scheduler logs
docker logs metalayer_scheduler

# Verify DAG syntax
airflow dags list
airflow dags show <dag_id>
```

### Emergency Procedures

#### Rollback Production Deployment
1. Navigate to GitHub Actions
2. Find the previous successful deployment
3. Click "Re-run jobs" to rollback
4. Or use manual workflow dispatch with previous image tag

#### Database Recovery
```bash
# Restore from backup (created before each deployment)
docker exec -i metalayer_postgres psql -U postgres airflow < backup_YYYYMMDD_HHMMSS.sql
```

## 📚 Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Documentation](https://prometheus.io/docs/)

## 🔄 Maintenance

### Regular Tasks
- **Weekly**: Review security scan results
- **Monthly**: Update base images and dependencies  
- **Quarterly**: Review and update deployment procedures
- **Annually**: Security audit and penetration testing

### Backup Verification
- **Daily**: Automated backup creation
- **Weekly**: Backup integrity verification
- **Monthly**: Disaster recovery testing