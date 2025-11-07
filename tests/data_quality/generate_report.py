"""
Data Quality Report Generator for MetaLayer

This script generates comprehensive data quality reports from test results
and database metrics. It creates HTML reports and JSON summaries for CI/CD.
"""

import json
import os
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))


class DataQualityReportGenerator:
    """Generate comprehensive data quality reports."""

    def __init__(self, database_url=None):
        self.database_url = database_url or os.getenv(
            "DATABASE_URL",
            "postgresql://test_user:test_password@localhost:5432/test_airflow",
        )
        self.reports_dir = project_root / "reports"
        self.reports_dir.mkdir(exist_ok=True)

    def get_database_connection(self):
        """Get database connection with error handling."""
        try:
            conn = psycopg2.connect(self.database_url)
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return None

    def collect_table_metrics(self, conn):
        """Collect basic metrics from all tables."""
        metrics = {}
        cursor = conn.cursor()

        # Define expected tables and their key columns
        tables_config = {
            "bronze.erp_sales_raw": ["sales_id", "customer_id", "sale_amount"],
            "bronze.crm_customers_raw": ["customer_id", "customer_name"],
            "silver.sales_cleaned": ["sales_id", "customer_id", "sale_amount_clean"],
            "silver.customers_standardized": ["customer_id", "customer_name_clean"],
            "gold.sales_analytics": ["metric_name", "metric_value"],
        }

        for table_name, expected_columns in tables_config.items():
            try:
                # Check if table exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    )
                    """,
                    tuple(table_name.split(".")),
                )
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]

                    # Get column info
                    schema, table = table_name.split(".")
                    cursor.execute(
                        """
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (schema, table),
                    )
                    columns = cursor.fetchall()

                    metrics[table_name] = {
                        "exists": True,
                        "row_count": row_count,
                        "columns": [
                            {"name": col[0], "type": col[1]} for col in columns
                        ],
                        "expected_columns": expected_columns,
                        "missing_columns": [
                            col
                            for col in expected_columns
                            if col not in [c[0] for c in columns]
                        ],
                    }
                else:
                    metrics[table_name] = {
                        "exists": False,
                        "row_count": 0,
                        "columns": [],
                        "expected_columns": expected_columns,
                        "missing_columns": expected_columns,
                    }

            except Exception as e:
                print(f"⚠️ Error checking table {table_name}: {e}")
                metrics[table_name] = {
                    "exists": False,
                    "error": str(e),
                    "expected_columns": expected_columns,
                }

        return metrics

    def check_data_quality_issues(self, conn):
        """Check for common data quality issues."""
        issues = []
        cursor = conn.cursor()

        quality_checks = [
            {
                "name": "Bronze Layer - Null Sales Amount",
                "query": "SELECT COUNT(*) FROM bronze.erp_sales_raw WHERE sale_amount IS NULL",
                "table": "bronze.erp_sales_raw",
            },
            {
                "name": "Bronze Layer - Invalid Sales Amount",
                "query": "SELECT COUNT(*) FROM bronze.erp_sales_raw WHERE sale_amount <= 0",
                "table": "bronze.erp_sales_raw",
            },
            {
                "name": "Silver Layer - Null Cleaned Amount",
                "query": "SELECT COUNT(*) FROM silver.sales_cleaned WHERE sale_amount_clean IS NULL",
                "table": "silver.sales_cleaned",
            },
            {
                "name": "Silver Layer - Invalid Cleaned Amount",
                "query": "SELECT COUNT(*) FROM silver.sales_cleaned WHERE sale_amount_clean <= 0",
                "table": "silver.sales_cleaned",
            },
            {
                "name": "Silver Layer - Duplicate Sales IDs",
                "query": """
                    SELECT COUNT(*) FROM (
                        SELECT sales_id, COUNT(*)
                        FROM silver.sales_cleaned
                        GROUP BY sales_id
                        HAVING COUNT(*) > 1
                    ) duplicates
                """,
                "table": "silver.sales_cleaned",
            },
        ]

        for check in quality_checks:
            try:
                cursor.execute(check["query"])
                issue_count = cursor.fetchone()[0]
                issues.append(
                    {
                        "name": check["name"],
                        "table": check["table"],
                        "issue_count": issue_count,
                        "status": "PASS" if issue_count == 0 else "FAIL",
                    }
                )
            except Exception as e:
                issues.append(
                    {
                        "name": check["name"],
                        "table": check["table"],
                        "issue_count": None,
                        "status": "ERROR",
                        "error": str(e),
                    }
                )

        return issues

    def generate_html_report(self, metrics, quality_issues, output_file):
        """Generate an HTML data quality report."""

        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>MetaLayer Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f8ff; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; }}
        .table-metrics {{ border-collapse: collapse; width: 100%; }}
        .table-metrics th, .table-metrics td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .table-metrics th {{ background-color: #f2f2f2; }}
        .status-pass {{ color: green; font-weight: bold; }}
        .status-fail {{ color: red; font-weight: bold; }}
        .status-error {{ color: orange; font-weight: bold; }}
        .missing-column {{ color: red; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏗️ MetaLayer Data Quality Report</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Environment: CI/CD Testing</p>
    </div>

    <div class="section">
        <h2>📊 Table Metrics Summary</h2>
        <table class="table-metrics">
            <thead>
                <tr>
                    <th>Table</th>
                    <th>Exists</th>
                    <th>Row Count</th>
                    <th>Columns</th>
                    <th>Missing Expected Columns</th>
                </tr>
            </thead>
            <tbody>
        """

        for table_name, metric in metrics.items():
            exists_status = "✅ Yes" if metric.get("exists", False) else "❌ No"
            row_count = metric.get("row_count", 0)
            column_count = len(metric.get("columns", []))
            missing_cols = metric.get("missing_columns", [])
            missing_display = ", ".join(missing_cols) if missing_cols else "None"
            missing_class = "missing-column" if missing_cols else ""

            html_content += f"""
                <tr>
                    <td>{table_name}</td>
                    <td>{exists_status}</td>
                    <td>{row_count:,}</td>
                    <td>{column_count}</td>
                    <td class="{missing_class}">{missing_display}</td>
                </tr>
            """

        html_content += """
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>🔍 Data Quality Checks</h2>
        <table class="table-metrics">
            <thead>
                <tr>
                    <th>Check Name</th>
                    <th>Table</th>
                    <th>Issue Count</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """

        for issue in quality_issues:
            status_class = f"status-{issue['status'].lower()}"
            issue_count = (
                issue["issue_count"] if issue["issue_count"] is not None else "N/A"
            )

            html_content += f"""
                <tr>
                    <td>{issue['name']}</td>
                    <td>{issue['table']}</td>
                    <td>{issue_count}</td>
                    <td class="{status_class}">{issue['status']}</td>
                </tr>
            """

        html_content += """
            </tbody>
        </table>
    </div>

    <div class="section">
        <h2>📈 Summary</h2>
        <ul>
        """

        total_checks = len(quality_issues)
        passed_checks = len([i for i in quality_issues if i["status"] == "PASS"])
        failed_checks = len([i for i in quality_issues if i["status"] == "FAIL"])
        error_checks = len([i for i in quality_issues if i["status"] == "ERROR"])

        total_tables = len(metrics)
        existing_tables = len([m for m in metrics.values() if m.get("exists", False)])

        html_content += f"""
            <li>Total tables expected: {total_tables}</li>
            <li>Tables existing: {existing_tables}</li>
            <li>Quality checks passed: {passed_checks}/{total_checks}</li>
            <li>Quality checks failed: {failed_checks}/{total_checks}</li>
            <li>Quality checks with errors: {error_checks}/{total_checks}</li>
        """

        html_content += """
        </ul>
    </div>

</body>
</html>
        """

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"📊 HTML report generated: {output_file}")

    def generate_json_summary(self, metrics, quality_issues, output_file):
        """Generate a JSON summary for programmatic use."""

        summary = {
            "timestamp": datetime.now().isoformat(),
            "environment": "ci_cd_testing",
            "table_metrics": metrics,
            "quality_checks": quality_issues,
            "summary_stats": {
                "total_tables_expected": len(metrics),
                "tables_existing": len(
                    [m for m in metrics.values() if m.get("exists", False)]
                ),
                "total_quality_checks": len(quality_issues),
                "checks_passed": len(
                    [i for i in quality_issues if i["status"] == "PASS"]
                ),
                "checks_failed": len(
                    [i for i in quality_issues if i["status"] == "FAIL"]
                ),
                "checks_with_errors": len(
                    [i for i in quality_issues if i["status"] == "ERROR"]
                ),
            },
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"📋 JSON summary generated: {output_file}")

    def generate_reports(self):
        """Generate all data quality reports."""
        print("=" * 60)
        print("📊 MetaLayer Data Quality Report Generation")
        print("=" * 60)

        # Get database connection
        conn = self.get_database_connection()
        if not conn:
            print("❌ Cannot generate reports without database connection")
            # Generate empty reports for CI/CD
            self.generate_empty_reports()
            return False

        try:
            # Collect metrics
            print("🔍 Collecting table metrics...")
            metrics = self.collect_table_metrics(conn)

            print("🧪 Running data quality checks...")
            quality_issues = self.check_data_quality_issues(conn)

            # Generate reports
            html_file = self.reports_dir / "data_quality_report.html"
            json_file = self.reports_dir / "data_profiling.json"

            print("📝 Generating HTML report...")
            self.generate_html_report(metrics, quality_issues, html_file)

            print("📋 Generating JSON summary...")
            self.generate_json_summary(metrics, quality_issues, json_file)

            # Print summary to console
            total_checks = len(quality_issues)
            passed_checks = len([i for i in quality_issues if i["status"] == "PASS"])
            failed_checks = len([i for i in quality_issues if i["status"] == "FAIL"])

            print(f"\n📈 Data Quality Summary:")
            print(f"   Quality checks passed: {passed_checks}/{total_checks}")
            print(f"   Quality checks failed: {failed_checks}/{total_checks}")

            if failed_checks > 0:
                print(f"\n⚠️ Data quality issues detected:")
                for issue in quality_issues:
                    if issue["status"] == "FAIL":
                        print(f"   - {issue['name']}: {issue['issue_count']} issues")

            print(f"\n✅ Data quality report generation completed!")
            return True

        except Exception as e:
            print(f"❌ Error generating reports: {e}")
            print(f"Stack trace: {traceback.format_exc()}")
            self.generate_empty_reports()
            return False
        finally:
            conn.close()

    def generate_empty_reports(self):
        """Generate empty reports when database is not available."""
        print("📝 Generating empty reports for CI/CD...")

        # Empty HTML report
        html_file = self.reports_dir / "data_quality_report.html"
        empty_html = f"""
<!DOCTYPE html>
<html>
<head><title>MetaLayer Data Quality Report - No Database</title></head>
<body>
    <h1>📊 MetaLayer Data Quality Report</h1>
    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p><strong>⚠️ Database not available during CI/CD run</strong></p>
    <p>This is a placeholder report for artifact upload purposes.</p>
</body>
</html>
        """
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(empty_html)

        # Empty JSON report
        json_file = self.reports_dir / "data_profiling.json"
        empty_json = {
            "timestamp": datetime.now().isoformat(),
            "environment": "ci_cd_testing",
            "status": "database_unavailable",
            "message": "Database connection not available during CI/CD execution",
        }
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(empty_json, f, indent=2)

        print(f"📊 Empty reports generated for CI/CD artifact upload")


def main():
    """Main function to generate data quality reports."""
    generator = DataQualityReportGenerator()
    success = generator.generate_reports()
    return success


if __name__ == "__main__":
    success = main()
    # Exit with appropriate code for CI/CD
    sys.exit(0 if success else 1)
