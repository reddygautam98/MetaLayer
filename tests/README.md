# Test configuration files for GitHub Actions workflows

# This directory contains test-specific configurations and sample data
# used by the automated testing workflows

# DAG Import Test
tests/dags/test_dag_imports.py

# DAG Structure Test  
tests/dags/test_dag_structure.py

# Task Testing
tests/dags/test_tasks.py

# Data Quality Sample Data
tests/data_quality/create_sample_data.sql

# Data Quality Tests
tests/data_quality/test_data_quality.py

# Integration Test Sample Data
tests/integration/sample_data/

# End-to-End Pipeline Tests
tests/integration/test_pipeline_e2e.py

# Pipeline Result Validation
tests/integration/validate_pipeline_results.py

# Performance Benchmarks
tests/performance/test_benchmarks.py

# Memory Usage Tests
tests/performance/test_memory_usage.py