# MetaLayer GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated CI/CD, testing, and deployment of the MetaLayer data pipeline.

## 📁 Workflow Files

### 🚀 `main-pipeline.yml`
**Primary CI/CD Pipeline**
- **Purpose**: Complete automated pipeline for code quality, testing, building, and deployment
- **Triggers**: 
  - Push to `main`, `develop`, `staging` branches
  - Pull requests to `main`, `develop`
  - Release publications
  - Manual workflow dispatch
- **Jobs**:
  - Code quality & security analysis
  - Airflow DAG validation
  - Docker image building & pushing
  - Staging deployment (develop branch)
  - Production deployment (main branch)
  - Cleanup & notifications

### 🛡️ `security-quality.yml`
**Security & Quality Assurance**
- **Purpose**: Comprehensive security scanning and code quality analysis
- **Triggers**:
  - Push/PR to main branches
  - Daily schedule (2 AM UTC)
  - Manual dispatch
- **Jobs**:
  - Dependency vulnerability scanning (Safety, Bandit)
  - Static code analysis (Semgrep)
  - Docker security scanning (Trivy, Hadolint)
  - Infrastructure security checks
  - Compliance reporting

### 🧪 `testing.yml`
**Data Quality & Testing**
- **Purpose**: Automated testing for DAGs and data quality validation
- **Triggers**:
  - Changes to `dags/`, `include/`, `tests/` directories
  - Weekday schedule (6 AM UTC)
  - Manual dispatch
- **Jobs**:
  - DAG unit tests and validation
  - Data quality checks with Great Expectations
  - Test coverage reporting

### 🎯 `production-deploy.yml`
**Production Deployment**
- **Purpose**: Secure, validated production deployments
- **Triggers**:
  - GitHub release publications
  - Manual workflow dispatch (with environment selection)
- **Jobs**:
  - Pre-deployment validation
  - Production image building
  - Environment-specific deployment
  - Health verification
  - Post-deployment tasks

## 🔧 Setup Requirements

### 1. Repository Secrets

Configure these secrets in your GitHub repository:

```bash
# Production Environment
PROD_POSTGRES_PASSWORD=<secure-password>
PROD_AIRFLOW_PASSWORD=<secure-password>
PROD_GRAFANA_PASSWORD=<secure-password>

# Staging Environment  
STAGING_POSTGRES_PASSWORD=<password>
STAGING_AIRFLOW_PASSWORD=<password>
STAGING_GRAFANA_PASSWORD=<password>

# Optional: Custom registry credentials
REGISTRY_USERNAME=<username>
REGISTRY_PASSWORD=<token>
```

### 2. Environment Protection

Set up GitHub environment protection rules:

**Production Environment:**
- Required reviewers: 2+
- Deployment branches: `main` only
- Wait timer: 5 minutes

**Staging Environment:**
- Required reviewers: 1
- Deployment branches: `develop`, `main`

### 3. Branch Protection

Configure branch protection rules:

**Main Branch:**
- Require PR reviews (2 reviewers)
- Require status checks to pass
- Require branches up to date
- Restrict pushes

**Develop Branch:**
- Require PR reviews (1 reviewer) 
- Require status checks to pass

## 🔄 Workflow Execution

### Development Flow

1. **Feature Development**
   ```bash
   git checkout -b feature/new-feature develop
   # Make changes
   git push origin feature/new-feature
   # Create PR to develop
   ```

2. **Staging Deployment**
   - PR merge to `develop` triggers automatic staging deployment
   - Health checks verify deployment success
   - Manual testing on staging environment

3. **Production Release**
   ```bash
   # Create release PR
   git checkout -b release/v1.2.0 develop
   # Update version, changelog
   git push origin release/v1.2.0
   # Create PR to main
   
   # After merge, create GitHub release
   gh release create v1.2.0 --title "MetaLayer v1.2.0" --notes "Release notes..."
   ```

### Manual Deployments

Trigger manual deployments via GitHub UI:
1. Go to Actions tab
2. Select workflow (e.g., "Production Deploy")
3. Click "Run workflow"
4. Select environment and options
5. Click "Run workflow"

## 📊 Monitoring & Reporting

### Workflow Artifacts

Each workflow generates artifacts for review:

- **Security Reports**: `security-scan-reports`
- **Test Coverage**: `coverage-reports`
- **Compliance Reports**: `compliance-report`
- **Data Quality**: `data-quality-reports`
- **Deployment Summary**: `test-summary-report`

### Status Monitoring

Monitor workflow status through:
- GitHub Actions dashboard
- Email notifications (on failure)
- Slack integration (if configured)
- Grafana dashboards (post-deployment)

### Success Metrics

Track deployment success with:
- Deployment frequency
- Lead time for changes
- Mean time to recovery
- Change failure rate

## 🚨 Troubleshooting

### Common Issues

**Workflow Failures:**
```bash
# Check workflow logs in GitHub Actions
# Verify secrets are configured
# Ensure branch protection allows workflow
```

**Deployment Issues:**
```bash
# Check service health endpoints
# Review Docker logs
# Verify environment configuration
```

**DAG Validation Errors:**
```bash
# Test DAG syntax locally
# Check import dependencies
# Verify connection configurations
```

### Emergency Procedures

**Rollback Deployment:**
1. Navigate to previous successful workflow
2. Use "Re-run jobs" option
3. Or trigger manual deployment with previous version

**Skip Quality Gates (Emergency Only):**
```yaml
# Add to workflow dispatch inputs
skip_tests:
  description: 'Skip test validation (emergency only)'
  required: false
  default: false
  type: boolean
```

## 🔒 Security Considerations

### Secret Management
- Use GitHub Secrets for sensitive data
- Rotate secrets regularly
- Limit secret access to necessary workflows
- Audit secret usage

### Access Control
- Implement environment protection rules
- Use required reviewers for production
- Limit workflow permissions
- Monitor deployment activities

### Container Security
- Scan images for vulnerabilities
- Use minimal base images
- Regular security updates
- Sign container images (optional)

## 📈 Performance Optimization

### Workflow Efficiency
- Use caching for dependencies
- Parallel job execution
- Conditional job execution
- Resource-appropriate runners

### Build Optimization
- Multi-stage Docker builds
- Layer caching strategies
- Dependency optimization
- Parallel test execution

## 📚 Additional Resources

- [GitHub Actions Best Practices](https://docs.github.com/en/actions/learn-github-actions/essential-features-of-github-actions)
- [Workflow Security](https://docs.github.com/en/actions/security-guides)
- [Environment Protection Rules](https://docs.github.com/en/actions/deployment/targeting-different-environments)
- [Secrets Management](https://docs.github.com/en/actions/security-guides/encrypted-secrets)