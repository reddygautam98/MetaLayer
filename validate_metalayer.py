#!/usr/bin/env python3
"""
MetaLayer Configuration Validation Script
Tests all major components and fixes implemented
"""

import sys
import importlib.util
from pathlib import Path


def test_syntax_compilation():
    """Test that all Python files compile without syntax errors"""
    print("🔍 Testing Python file compilation...")

    test_files = [
        "dags/medallion_master_orchestrator.py",
        "dags/bronze_layer_production.py",
        "dags/silver_layer_production.py",
        "dags/gold_layer_production.py",
        "dags/data_quality_monitoring.py",
        "include/utils/data_quality_monitoring.py",
        "include/utils/metrics_exporter.py",
    ]

    results = []
    for file_path in test_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                compile(f.read(), file_path, "exec")
            results.append(f"✅ {file_path}")
        except SyntaxError as e:
            results.append(f"❌ {file_path}: Syntax Error - {e}")
        except FileNotFoundError:
            results.append(f"⚠️  {file_path}: File not found")
        except Exception as e:
            results.append(f"❌ {file_path}: {e}")

    return results


def test_data_files():
    """Test that required data files exist"""
    print("📁 Testing data file availability...")

    required_files = [
        "data/bronze_src/erp/erp_sales.csv",
        "data/bronze_src/crm/crm_customers.csv",
    ]

    results = []
    for file_path in required_files:
        path = Path(file_path)
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            results.append(f"✅ {file_path} (Size: {size_mb:.2f} MB)")
        else:
            results.append(f"❌ {file_path}: Missing")

    return results


def test_sql_files():
    """Test SQL file syntax and content"""
    print("🗄️ Testing SQL files...")

    sql_files = [
        "include/sql/init_schemas_pg.sql",
        "include/sql/bronze_ddl_pg.sql",
        "include/sql/silver_transform_pg.sql",
        "include/sql/gold_model_pg.sql",
    ]

    results = []
    for file_path in sql_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    results.append(f"✅ {file_path} ({len(content.split())} words)")
                else:
                    results.append(f"⚠️  {file_path}: Empty file")
        except FileNotFoundError:
            results.append(f"❌ {file_path}: File not found")
        except Exception as e:
            results.append(f"❌ {file_path}: {e}")

    return results


def test_configuration_files():
    """Test configuration files"""
    print("⚙️ Testing configuration files...")

    config_files = ["requirements.txt", "docker-compose.yml", "airflow_settings.yaml"]

    results = []
    for file_path in config_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    results.append(f"✅ {file_path}")
                else:
                    results.append(f"⚠️  {file_path}: Empty file")
        except FileNotFoundError:
            results.append(f"❌ {file_path}: File not found")
        except Exception as e:
            results.append(f"❌ {file_path}: {e}")

    return results


def check_requirements_consistency():
    """Check for duplicate or conflicting requirements"""
    print("📦 Testing requirements consistency...")

    try:
        with open("requirements.txt", "r") as f:
            lines = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]

        packages = {}
        duplicates = []

        for line in lines:
            if "==" in line:
                package = line.split("==")[0]
                if package in packages:
                    duplicates.append(package)
                else:
                    packages[package] = line

        if duplicates:
            return [f"❌ Duplicate packages found: {', '.join(duplicates)}"]
        else:
            return [f"✅ Requirements.txt clean ({len(packages)} packages)"]

    except Exception as e:
        return [f"❌ Error checking requirements: {e}"]


def main():
    """Run all validation tests"""
    print("🚀 MetaLayer Validation Report")
    print("=" * 50)

    all_results = []

    # Run all tests
    test_functions = [
        test_syntax_compilation,
        test_data_files,
        test_sql_files,
        test_configuration_files,
        check_requirements_consistency,
    ]

    for test_func in test_functions:
        results = test_func()
        all_results.extend(results)
        print()

    # Summary
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    success_count = len([r for r in all_results if r.startswith("✅")])
    warning_count = len([r for r in all_results if r.startswith("⚠️")])
    error_count = len([r for r in all_results if r.startswith("❌")])

    for result in all_results:
        print(result)

    print()
    print(f"✅ Passed: {success_count}")
    print(f"⚠️  Warnings: {warning_count}")
    print(f"❌ Errors: {error_count}")
    print()

    if error_count == 0:
        print("🎉 ALL TESTS PASSED! MetaLayer is ready for deployment.")
        return 0
    else:
        print("🔧 Some issues found. Please review and fix the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
