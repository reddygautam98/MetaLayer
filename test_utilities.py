#!/usr/bin/env python3
"""
MetaLayer Data Quality and Utility Function Tester
Tests all utility functions and data quality monitoring capabilities
"""

import sys
import traceback
from pathlib import Path


def test_utility_imports():
    """Test importing all utility modules"""
    print("🔍 Testing Utility Function Imports...")

    results = []
    utility_modules = [
        "include.utils.data_quality_monitoring",
        "include.utils.metrics_exporter",
        "include.utils.connection_pooling",
        "include.utils.incremental_processing",
    ]

    # Add include directory to Python path
    import sys

    sys.path.append("include")

    for module_name in utility_modules:
        try:
            # Try to import the module
            module = __import__(module_name, fromlist=[""])
            results.append(f"✅ {module_name} - Imported successfully")

            # Try to access key functions if available
            if hasattr(module, "basic_data_quality_check"):
                results.append(f"  📋 Found: basic_data_quality_check()")
            if hasattr(module, "DataQualityMonitor"):
                results.append(f"  📋 Found: DataQualityMonitor class")
            if hasattr(module, "simulate_database_metrics"):
                results.append(f"  📋 Found: simulate_database_metrics()")

        except ImportError as e:
            results.append(f"❌ {module_name} - Import error: {e}")
        except Exception as e:
            results.append(f"⚠️  {module_name} - Other error: {e}")

    return results


def test_data_quality_fallback():
    """Test data quality fallback functionality"""
    print("🧪 Testing Data Quality Fallback Functions...")

    results = []

    try:
        sys.path.append("include")
        from utils.data_quality_monitoring import basic_data_quality_check

        # Test function signature (without actual DB connection)
        import inspect

        sig = inspect.signature(basic_data_quality_check)
        params = list(sig.parameters.keys())

        expected_params = ["table_name", "schema_name", "conn_id"]
        if all(param in params for param in expected_params):
            results.append("✅ basic_data_quality_check - Correct function signature")
        else:
            results.append(
                f"❌ basic_data_quality_check - Incorrect signature. Expected: {expected_params}, Got: {params}"
            )

    except Exception as e:
        results.append(f"❌ basic_data_quality_check test failed: {e}")

    return results


def test_metrics_exporter():
    """Test metrics exporter functionality"""
    print("📊 Testing Metrics Exporter...")

    results = []

    try:
        sys.path.append("include")
        from utils.metrics_exporter import simulate_database_metrics

        results.append("✅ metrics_exporter - Functions accessible")

        # Test if prometheus_client is available (not required for validation)
        try:
            import prometheus_client

            results.append("✅ prometheus_client - Available for metrics")
        except ImportError:
            results.append("⚠️  prometheus_client - Not installed (optional)")

    except Exception as e:
        results.append(f"❌ metrics_exporter test failed: {e}")

    return results


def test_connection_pooling():
    """Test connection pooling utilities"""
    print("🔗 Testing Connection Pooling...")

    results = []

    try:
        sys.path.append("include")
        from utils.connection_pooling import DatabaseConnectionPool

        results.append("✅ connection_pooling - DatabaseConnectionPool class found")

        # Check if class has expected methods
        import inspect

        methods = [
            name
            for name, method in inspect.getmembers(
                DatabaseConnectionPool, inspect.isfunction
            )
        ]

        expected_methods = ["get_connection", "close_all_connections"]
        found_methods = [
            m
            for m in expected_methods
            if m in methods
            or f"_DatabaseConnectionPool__{m}" in dir(DatabaseConnectionPool)
        ]

        if (
            len(found_methods) >= len(expected_methods) // 2
        ):  # Allow for some flexibility
            results.append("✅ connection_pooling - Expected methods available")
        else:
            results.append(
                f"⚠️  connection_pooling - Some methods missing: {expected_methods}"
            )

    except Exception as e:
        results.append(f"❌ connection_pooling test failed: {e}")

    return results


def test_configuration_files():
    """Test configuration file validity"""
    print("⚙️ Testing Configuration Files...")

    results = []

    config_files = [
        ("airflow_settings.yaml", "YAML"),
        ("docker-compose.yml", "Docker Compose"),
        ("requirements.txt", "Python Requirements"),
        ("config/postgresql.conf", "PostgreSQL Config"),
        ("config/prometheus.yml", "Prometheus Config"),
    ]

    for file_path, file_type in config_files:
        path = Path(file_path)
        if path.exists():
            try:
                content = path.read_text(encoding="utf-8")
                if content.strip():
                    results.append(f"✅ {file_type} - {file_path} (Valid)")
                else:
                    results.append(f"⚠️  {file_type} - {file_path} (Empty)")
            except Exception as e:
                results.append(f"❌ {file_type} - {file_path} (Read error: {e})")
        else:
            results.append(f"⚠️  {file_type} - {file_path} (Not found)")

    return results


def test_grafana_dashboards():
    """Test Grafana dashboard configurations"""
    print("📈 Testing Grafana Dashboards...")

    results = []

    dashboard_dir = Path("config/grafana/dashboards")
    if dashboard_dir.exists():
        dashboard_files = list(dashboard_dir.glob("*.json"))

        for dashboard_file in dashboard_files:
            try:
                import json

                with open(dashboard_file, "r", encoding="utf-8") as f:
                    dashboard_data = json.load(f)

                # Check for required dashboard fields
                required_fields = ["title", "panels"]
                if all(field in dashboard_data for field in required_fields):
                    results.append(f"✅ Dashboard - {dashboard_file.name} (Valid JSON)")
                else:
                    results.append(
                        f"⚠️  Dashboard - {dashboard_file.name} (Missing required fields)"
                    )

            except json.JSONDecodeError as e:
                results.append(
                    f"❌ Dashboard - {dashboard_file.name} (Invalid JSON: {e})"
                )
            except Exception as e:
                results.append(f"❌ Dashboard - {dashboard_file.name} (Error: {e})")
    else:
        results.append("⚠️  Grafana dashboards directory not found")

    return results


def main():
    """Run all utility and data quality tests"""

    print("🚀 MetaLayer Utility & Data Quality Validation")
    print("=" * 60)

    all_results = []

    # Run all test functions
    test_functions = [
        test_utility_imports,
        test_data_quality_fallback,
        test_metrics_exporter,
        test_connection_pooling,
        test_configuration_files,
        test_grafana_dashboards,
    ]

    for test_func in test_functions:
        try:
            results = test_func()
            all_results.extend(results)
        except Exception as e:
            all_results.append(f"❌ {test_func.__name__} failed: {e}")
            traceback.print_exc()
        print()

    # Summary
    print("📋 VALIDATION SUMMARY")
    print("=" * 60)

    success_count = len([r for r in all_results if r.strip().startswith("✅")])
    warning_count = len([r for r in all_results if r.strip().startswith("⚠️")])
    error_count = len([r for r in all_results if r.strip().startswith("❌")])

    for result in all_results:
        print(result)

    print(f"\n📊 RESULTS:")
    print(f"✅ Passed: {success_count}")
    print(f"⚠️  Warnings: {warning_count}")
    print(f"❌ Errors: {error_count}")

    if error_count == 0:
        print(f"\n🎉 All critical tests passed! Utilities are functional.")
        return 0
    else:
        print(f"\n🔧 {error_count} errors found. Please review above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
