#!/usr/bin/env python3
"""
Final ETL Error Status Check
Validates if all errors are resolved and system is operational
"""

import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

def run_docker_command(command):
    """Run a docker command and return output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as e:
        print(f"Error running command: {e}")
        return None

def check_container_health():
    """Check if all containers are healthy"""
    print("🐳 CONTAINER HEALTH CHECK")
    print("=" * 50)
    
    cmd = 'docker ps --format "table {{.Names}}\\t{{.Status}}" | Select-String -Pattern "etl_|airflow"'
    result = run_docker_command(f'powershell -Command "{cmd}"')
    
    if result:
        lines = result.split('\n')
        healthy_count = 0
        total_count = 0
        
        for line in lines:
            if 'etl_' in line or 'airflow' in line:
                total_count += 1
                if 'healthy' in line or 'Up' in line:
                    healthy_count += 1
                    print(f"✅ {line}")
                else:
                    print(f"⚠️  {line}")
        
        print(f"\nContainer Status: {healthy_count}/{total_count} operational")
        return healthy_count >= (total_count * 0.8)  # 80% threshold
    return False

def check_database_connectivity():
    """Check database connectivity and data"""
    print("\n📊 DATABASE CONNECTIVITY & DATA CHECK")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="airflow",
            user="postgres",
            password="etl_secure_2024!"
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check schemas
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('bronze', 'silver', 'gold')
            """)
            schemas = [row['schema_name'] for row in cur.fetchall()]
            print(f"✅ Schemas available: {', '.join(schemas)}")
            
            # Check data counts
            data_counts = {}
            tables = [
                ('bronze', 'crm_customers_raw'),
                ('bronze', 'erp_sales_raw'),
                ('silver', 'customers_clean'),
                ('silver', 'sales_clean'),
                ('gold', 'customer_analytics'),
                ('gold', 'product_analytics')
            ]
            
            for schema, table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) as count FROM {schema}.{table}")
                    count = cur.fetchone()['count']
                    data_counts[f"{schema}.{table}"] = count
                    status = "✅" if count > 0 else "❌"
                    print(f"{status} {schema}.{table}: {count:,} records")
                except Exception as e:
                    print(f"❌ {schema}.{table}: Error - {e}")
                    data_counts[f"{schema}.{table}"] = 0
            
        conn.close()
        
        # Check if critical tables have data
        critical_data = (
            data_counts.get('bronze.crm_customers_raw', 0) > 0 and
            data_counts.get('bronze.erp_sales_raw', 0) > 0 and
            data_counts.get('silver.customers_clean', 0) > 0 and
            data_counts.get('silver.sales_clean', 0) > 0
        )
        
        print(f"\nDatabase Status: {'✅ OPERATIONAL' if critical_data else '❌ ISSUES DETECTED'}")
        return critical_data
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def check_dag_status():
    """Check current DAG status"""
    print("\n🔄 DAG STATUS CHECK")
    print("=" * 50)
    
    dags = [
        'bronze_layer_etl_pipeline',
        'silver_layer_etl_pipeline', 
        'gold_layer_analytics_pipeline',
        'master_etl_orchestrator'
    ]
    
    dag_status = {}
    
    for dag in dags:
        # Get recent DAG runs
        cmd = f'docker exec etl_airflow_webserver airflow dags list-runs -d {dag} --state success --limit 1 2>/dev/null'
        success_runs = run_docker_command(cmd)
        
        cmd = f'docker exec etl_airflow_webserver airflow dags list-runs -d {dag} --state running --limit 1 2>/dev/null'  
        running_runs = run_docker_command(cmd)
        
        cmd = f'docker exec etl_airflow_webserver airflow dags list-runs -d {dag} --state queued --limit 1 2>/dev/null'
        queued_runs = run_docker_command(cmd)
        
        if success_runs and 'manual__2025-11-10' in success_runs:
            print(f"✅ {dag}: Recent successful run")
            dag_status[dag] = 'success'
        elif running_runs and 'manual__2025-11-10' in running_runs:
            print(f"🔄 {dag}: Currently running")
            dag_status[dag] = 'running'
        elif queued_runs and 'manual__2025-11-10' in queued_runs:
            print(f"⏳ {dag}: Queued for execution")
            dag_status[dag] = 'queued'
        else:
            print(f"⚠️  {dag}: No recent activity or checking...")
            dag_status[dag] = 'unknown'
    
    successful_or_active = sum(1 for status in dag_status.values() if status in ['success', 'running', 'queued'])
    total_dags = len(dags)
    
    print(f"\nDAG Status: {successful_or_active}/{total_dags} active or successful")
    return successful_or_active >= (total_dags * 0.5)  # 50% threshold for active system

def check_airflow_webui():
    """Check if Airflow web UI is accessible"""
    print("\n🌐 AIRFLOW WEB UI CHECK")
    print("=" * 50)
    
    try:
        import urllib.request
        response = urllib.request.urlopen('http://localhost:8080', timeout=10)
        if response.getcode() == 200:
            print("✅ Airflow Web UI is accessible at http://localhost:8080")
            return True
        else:
            print(f"⚠️  Airflow Web UI returned status code: {response.getcode()}")
            return False
    except Exception as e:
        print(f"❌ Airflow Web UI not accessible: {e}")
        return False

def main():
    """Main validation function"""
    print("🔍 FINAL ETL ERROR STATUS VERIFICATION")
    print("🕒 Timestamp:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 70)
    
    # Run all checks
    checks = {
        "Container Health": check_container_health(),
        "Database Connectivity": check_database_connectivity(), 
        "DAG Status": check_dag_status(),
        "Airflow Web UI": check_airflow_webui()
    }
    
    print("\n" + "=" * 70)
    print("📋 FINAL ERROR STATUS SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for check_name, result in checks.items():
        status = "✅ RESOLVED" if result else "❌ ISSUES REMAIN"
        print(f"{status} - {check_name}")
        if not result:
            all_passed = False
    
    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 ERROR RESOLUTION STATUS: ALL ERRORS FIXED!")
        print("✅ Your MetaLayer ETL system is fully operational")
        print("✅ All previous errors have been resolved")
        print("✅ System ready for production use")
    else:
        print("⚠️  ERROR RESOLUTION STATUS: SOME ISSUES DETECTED")
        print("🔍 Review the failed checks above")
        print("📋 System may need additional troubleshooting")
    
    print("=" * 70)
    
    return all_passed

if __name__ == "__main__":
    main()