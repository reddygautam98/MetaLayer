#!/usr/bin/env python3
"""
Grafana Comprehensive Feature Test - UI Based Validation
Tests Grafana functionality and current data usage through direct validation
"""

import requests
import json
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuration
GRAFANA_URL = "http://localhost:3000"
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'airflow',
    'user': 'postgres',
    'password': 'etl_secure_2024!'
}

def check_grafana_accessibility():
    """Check if Grafana web interface is accessible"""
    print("🌐 GRAFANA WEB INTERFACE CHECK")
    print("=" * 50)
    
    try:
        response = requests.get(GRAFANA_URL, timeout=10)
        if response.status_code in [200, 302]:  # 302 is redirect to login
            print("✅ Grafana web interface is accessible")
            print(f"   📊 Status: {response.status_code}")
            print(f"   🌐 URL: {GRAFANA_URL}")
            return True
        else:
            print(f"❌ Unexpected status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Grafana not accessible: {e}")
        return False

def check_dashboard_files():
    """Check if dashboard files exist and are valid"""
    print("\n📋 DASHBOARD FILES VALIDATION")
    print("=" * 50)
    
    dashboard_dir = r"c:\Users\reddy\Downloads\MetaLayer\config\grafana\dashboards"
    
    try:
        import os
        dashboards_found = []
        
        for file in os.listdir(dashboard_dir):
            if file.endswith('.json'):
                file_path = os.path.join(dashboard_dir, file)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        dashboard_data = json.load(f)
                        
                    title = dashboard_data.get('title', 'Unknown')
                    panels = len(dashboard_data.get('panels', []))
                    
                    print(f"✅ Dashboard: {file}")
                    print(f"   📊 Title: {title}")
                    print(f"   🔢 Panels: {panels}")
                    
                    dashboards_found.append({
                        'file': file,
                        'title': title,
                        'panels': panels
                    })
                    
                except json.JSONDecodeError as e:
                    print(f"❌ Invalid JSON in {file}: {e}")
                
        print(f"\n✅ Total Valid Dashboards: {len(dashboards_found)}")
        return dashboards_found
        
    except Exception as e:
        print(f"❌ Dashboard validation failed: {e}")
        return []

def check_datasource_configuration():
    """Check datasource configuration files"""
    print("\n🔗 DATASOURCE CONFIGURATION CHECK")
    print("=" * 50)
    
    datasource_file = r"c:\Users\reddy\Downloads\MetaLayer\config\grafana\provisioning\datasources\datasources.yml"
    
    try:
        import yaml
        with open(datasource_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        datasources = config.get('datasources', [])
        print(f"✅ Datasource configuration loaded")
        print(f"   📂 File: datasources.yml") 
        print(f"   🔢 Datasources configured: {len(datasources)}")
        
        for ds in datasources:
            name = ds.get('name', 'Unknown')
            ds_type = ds.get('type', 'Unknown')
            url = ds.get('url', 'Not specified')
            print(f"   📊 {name} ({ds_type}): {url}")
            
        return len(datasources) > 0
        
    except FileNotFoundError:
        print("❌ Datasource configuration file not found")
        return False
    except Exception as e:
        print(f"❌ Datasource configuration error: {e}")
        return False

def validate_current_data():
    """Validate that current/fresh data is available"""
    print("\n📊 CURRENT DATA VALIDATION")
    print("=" * 50)
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check ETL data volumes
            cur.execute("""
                SELECT 
                    schemaname,
                    relname as tablename,
                    n_live_tup as records
                FROM pg_stat_user_tables 
                WHERE schemaname IN ('bronze', 'silver', 'gold')
                ORDER BY schemaname, relname;
            """)
            
            tables = cur.fetchall()
            
            bronze_total = 0
            silver_total = 0 
            gold_total = 0
            
            print("✅ ETL Data Layer Status:")
            
            for table in tables:
                schema = table['schemaname']
                table_name = table['tablename'] 
                records = table['records']
                
                if schema == 'bronze':
                    bronze_total += records
                elif schema == 'silver':
                    silver_total += records
                elif schema == 'gold':
                    gold_total += records
                    
                print(f"   {schema}.{table_name}: {records:,} records")
            
            print(f"\n✅ Layer Totals:")
            print(f"   🥉 Bronze: {bronze_total:,} records")
            print(f"   🥈 Silver: {silver_total:,} records") 
            print(f"   🥇 Gold: {gold_total:,} records")
            
            # Check business analytics freshness
            cur.execute("""
                SELECT 
                    COUNT(*) as analytics_count,
                    MAX(analytics_timestamp) as latest_update
                FROM gold.customer_analytics 
                WHERE analytics_timestamp IS NOT NULL;
            """)
            
            analytics = cur.fetchone()
            analytics_count = analytics['analytics_count']
            latest_update = analytics['latest_update']
            
            if latest_update:
                time_diff = datetime.now() - latest_update.replace(tzinfo=None)
                hours_old = time_diff.total_seconds() / 3600
                
                print(f"\n✅ Business Analytics Status:")
                print(f"   📈 Analytics records: {analytics_count}")
                print(f"   ⏰ Last update: {latest_update}")
                print(f"   🕐 Age: {hours_old:.1f} hours")
                
                if hours_old < 24:
                    print("   ✅ Data is fresh (< 24 hours)")
                else:
                    print("   ⚠️  Data is aging (> 24 hours)")
            
            # Check revenue data
            cur.execute("""
                SELECT 
                    SUM(total_revenue) as total_revenue,
                    COUNT(DISTINCT region) as regions,
                    COUNT(*) as analytics_records
                FROM gold.customer_analytics;
            """)
            
            revenue = cur.fetchone()
            total_revenue = revenue['total_revenue']
            regions = revenue['regions']
            analytics_records = revenue['analytics_records']
            
            print(f"\n✅ Business Intelligence Data:")
            print(f"   💰 Total Revenue: ${total_revenue:,.2f}")
            print(f"   🌍 Regions: {regions}")
            print(f"   📊 Analytics Records: {analytics_records}")
            
        conn.close()
        
        # Data quality assessment
        data_quality = "EXCELLENT"
        if bronze_total == 0:
            data_quality = "POOR - No source data"
        elif gold_total == 0:
            data_quality = "POOR - No analytics" 
        elif hours_old > 48:
            data_quality = "FAIR - Data aging"
        elif bronze_total < 1000:
            data_quality = "GOOD - Limited data"
            
        print(f"\n✅ Overall Data Quality: {data_quality}")
        return True
        
    except Exception as e:
        print(f"❌ Data validation failed: {e}")
        return False

def check_prometheus_integration():
    """Check Prometheus integration for monitoring"""
    print("\n📊 PROMETHEUS MONITORING CHECK")
    print("=" * 50)
    
    try:
        response = requests.get("http://localhost:9090/api/v1/query?query=up", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                metrics = data['data']['result']
                
                print(f"✅ Prometheus integration active")
                print(f"   📊 Metrics available: {len(metrics)}")
                
                services_up = 0
                for metric in metrics:
                    job = metric['metric'].get('job', 'unknown')
                    value = float(metric['value'][1])
                    status = "UP ✅" if value == 1 else "DOWN ❌"
                    print(f"   🚀 {job}: {status}")
                    if value == 1:
                        services_up += 1
                
                print(f"\n✅ Service Health: {services_up}/{len(metrics)} services UP")
                return True
            else:
                print(f"❌ Prometheus query failed: {data}")
                return False
        else:
            print(f"❌ Prometheus not accessible: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Prometheus check failed: {e}")
        return False

def generate_feature_report():
    """Generate comprehensive feature validation report"""
    print("\n" + "=" * 70)
    print("📋 GRAFANA FEATURE & FUNCTIONALITY REPORT")
    print("=" * 70)
    
    # Run all checks
    checks = [
        ("Web Interface", check_grafana_accessibility),
        ("Dashboard Files", lambda: len(check_dashboard_files()) > 0),
        ("Datasource Config", check_datasource_configuration), 
        ("Current Data", validate_current_data),
        ("Prometheus Integration", check_prometheus_integration)
    ]
    
    results = {}
    
    for check_name, check_func in checks:
        try:
            result = check_func()
            results[check_name] = result
        except Exception as e:
            print(f"❌ {check_name} check failed: {e}")
            results[check_name] = False
    
    # Summary
    print(f"\n{'='*70}")
    print("🎯 GRAFANA FUNCTIONALITY ASSESSMENT")
    print("=" * 70)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for check_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} - {check_name}")
    
    print(f"\n📊 Overall Score: {passed}/{total} checks passed")
    
    if passed == total:
        print("🎉 GRAFANA STATUS: FULLY FUNCTIONAL")
        print("✅ All features operational with current data")
        print("✅ Ready for real-time monitoring")
        print("✅ Business intelligence dashboards configured")
    elif passed >= total * 0.8:  # 80% or more
        print("✅ GRAFANA STATUS: MOSTLY FUNCTIONAL") 
        print("⚠️  Minor issues detected")
        print("✅ Core features operational")
    else:
        print("⚠️  GRAFANA STATUS: NEEDS ATTENTION")
        failed_checks = [name for name, result in results.items() if not result]
        print(f"❌ Failed: {', '.join(failed_checks)}")
    
    print("=" * 70)
    print("🌐 Grafana Web UI: http://localhost:3000")
    print("📊 Default Login: admin / admin (if required)")
    print("📋 Dashboard Location: /config/grafana/dashboards/")
    print("🔗 Data Sources: PostgreSQL ETL + Prometheus")
    print("=" * 70)
    
    return passed, total

if __name__ == "__main__":
    print("🚀 GRAFANA COMPREHENSIVE FEATURE VALIDATION")
    print("🕒 Timestamp:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 70)
    
    passed, total = generate_feature_report()
    
    if passed == total:
        exit(0)  # Success
    else:
        exit(1)  # Issues detected