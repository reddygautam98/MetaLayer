# Contributing to MetaLayer

Thank you for your interest in contributing to MetaLayer! This document provides guidelines for contributing to the project.

## 🎯 How to Contribute

### Reporting Issues
- Use the appropriate issue template
- Provide detailed information about your environment
- Include steps to reproduce the issue
- Add relevant logs and error messages

### Feature Requests
- Describe the problem you're trying to solve
- Explain your proposed solution
- Provide use cases and examples
- Consider alternative approaches

### Code Contributions
1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

## 📋 Development Guidelines

### Code Style
- Follow PEP 8 for Python code
- Use Black for code formatting
- Use isort for import sorting
- Add type hints where applicable
- Write descriptive commit messages

### Testing
- Write unit tests for new functionality
- Ensure all existing tests pass
- Test DAGs with `airflow dags test`
- Validate Docker builds locally

### Documentation
- Update relevant documentation
- Add docstrings to functions and classes
- Update USERGUIDE.md for user-facing changes
- Include examples where helpful

## 🏗️ Development Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Git

### Local Development
```bash
# Clone the repository
git clone https://github.com/reddygautam98/MetaLayer.git
cd MetaLayer

# Create environment file
cp .env.template .env
# Edit .env with your settings

# Start development environment
docker-compose -f docker-compose-fast.yml up -d

# Access services
# Airflow: http://localhost:8080
# Grafana: http://localhost:3000
```

### Running Tests
```bash
# Code quality checks
black --check dags/ include/
flake8 dags/ include/
pylint dags/ include/

# DAG validation
airflow dags list-import-errors

# Security scan
bandit -r dags/ include/
```

## 🔍 Pull Request Process

### Before Submitting
- [ ] Code follows style guidelines
- [ ] Tests pass locally
- [ ] Documentation is updated
- [ ] Security scan passes
- [ ] DAGs validate successfully

### PR Template
When creating a PR, please:
- Use a descriptive title
- Reference related issues
- Describe changes made
- Include testing notes
- Add screenshots (if UI changes)

### Review Process
1. Automated checks must pass
2. Code review by maintainers
3. Testing in staging environment
4. Final approval and merge

## 📊 Component Architecture

### ETL Layers
- **Bronze**: Raw data ingestion
- **Silver**: Data cleaning and transformation  
- **Gold**: Analytics and aggregations

### Infrastructure
- **Airflow**: Workflow orchestration
- **PostgreSQL**: Data storage
- **Grafana/Prometheus**: Monitoring
- **Docker**: Containerization

## 🛡️ Security Guidelines

- Never commit secrets or passwords
- Use environment variables for configuration
- Follow security scanning results
- Report security issues privately
- Use official base images

## 📚 Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [MetaLayer User Guide](./USERGUIDE.md)

## 💬 Getting Help

- Create an issue for questions
- Check existing documentation
- Review similar issues/PRs
- Join community discussions

## 🏆 Recognition

Contributors will be recognized in:
- CONTRIBUTORS.md file
- Release notes
- Project documentation

Thank you for contributing to MetaLayer! 🚀