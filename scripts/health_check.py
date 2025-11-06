#!/usr/bin/env python3
"""Health check script for Airflow ETL pipeline."""
import sys
import subprocess

def check_airflow_process():
    """Check if Airflow processes are running."""
    try:
        result = subprocess.run(['pgrep', '-f', 'airflow'], capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False

def check_webserver_health():
    """Check Airflow webserver health."""
    try:
        import urllib.request
        urllib.request.urlopen('http://localhost:8080/health', timeout=5)
        return True
    except Exception:
        return False

def check_database_connection():
    """Check database connectivity."""
    try:
        result = subprocess.run(
            ['airflow', 'db', 'check'], 
            capture_output=True, 
            timeout=10
        )
        return result.returncode == 0
    except Exception:
        return False

if __name__ == '__main__':
    checks = [
        ('Airflow Process', check_airflow_process),
        ('Database Connection', check_database_connection)
    ]
    
    for name, check_func in checks:
        if not check_func():
            print(f"Health check failed: {name}")
            sys.exit(1)
    
    print("All health checks passed")
    sys.exit(0)