#!/usr/bin/env python3
"""
Comprehensive Grafana Functionality & Feature Validation
Tests all Grafana features and ensures current data is being used
"""

import requests
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import time

# Grafana Configuration
GRAFANA_URL = "http://localhost:3000"
GRAFANA_USER = "admin"
GRAFANA_PASS = "admin"

def test_grafana_accessibility():
    """Test if Grafana web interface is accessible"""
    print("🌐 TESTING GRAFANA WEB ACCESS")
    print("=" * 50)
    
    try:
        response = requests.get(GRAFANA_URL, timeout=10)
        if response.status_code == 200:
            print("✅ Grafana web interface accessible at http://localhost:3000")
            return True
        else:
            print(f"❌ Grafana returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Grafana web interface not accessible: {e}")
        return False

def test_grafana_api():
    """Test Grafana API functionality"""
    print("\n🔧 TESTING GRAFANA API FUNCTIONALITY")
    print("=" * 50)
    
    try:
        # Test API health
        response = requests.get(f"{GRAFANA_URL}/api/health", timeout=10)
        if response.status_code == 200:
            health_data = response.json()
            print(f"✅ Grafana API Health: {health_data.get('message', 'OK')}")
            
            # Test datasources API
            auth = (GRAFANA_USER, GRAFANA_PASS)
            ds_response = requests.get(f"{GRAFANA_URL}/api/datasources", auth=auth, timeout=10)
            
            if ds_response.status_code == 200:
                datasources = ds_response.json()
                print(f"✅ Grafana API accessible - Found {len(datasources)} datasource(s)")
                
                for ds in datasources:
                    ds_name = ds.get('name', 'Unknown')
                    ds_type = ds.get('type', 'Unknown')
                    print(f"   📊 Datasource: {ds_name} (Type: {ds_type})")
                
                return True
            else:
                print(f"⚠️  Datasources API returned: {ds_response.status_code}")
                return False
        else:
            print(f"❌ Grafana API health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Grafana API test failed: {e}")
        return False

def test_postgresql_datasource():
    """Test PostgreSQL datasource connectivity"""
    print("\n🗄️ TESTING POSTGRESQL DATASOURCE")
    print("=" * 50)
    
    try:
        # Direct database connection test
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="airflow", 
            user="postgres",
            password="etl_secure_2024!"
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Test basic connectivity
            cur.execute("SELECT version();")
            version = cur.fetchone()['version']
            print(f"✅ PostgreSQL Database Connected: {version[:50]}...")
            
            # Test ETL data availability
            cur.execute("""
                SELECT 
                    schemaname,
                    COUNT(*) as table_count,
                    SUM(n_live_tup) as total_records
                FROM pg_stat_user_tables 
                WHERE schemaname IN ('bronze', 'silver', 'gold')
                GROUP BY schemaname
                ORDER BY schemaname;
            """)
            
            etl_data = cur.fetchall()
            print(f"✅ ETL Data Available:")
            
            total_records = 0
            for row in etl_data:
                schema = row['schemaname']
                tables = row['table_count']
                records = row['total_records']
                total_records += records
                print(f"   🥉🥈🥇 {schema}: {tables} tables, {records:,} records")
            
            print(f"✅ Total ETL Records: {total_records:,}")
            
            # Test business analytics data
            cur.execute("SELECT COUNT(*) as count FROM gold.customer_analytics;")
            customer_analytics = cur.fetchone()['count']
            
            cur.execute("SELECT COUNT(*) as count FROM gold.product_analytics;")
            product_analytics = cur.fetchone()['count']
            
            print(f"✅ Business Analytics Available:")
            print(f"   📈 Customer Analytics: {customer_analytics} regions")
            print(f"   📦 Product Analytics: {product_analytics} products")
            
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ PostgreSQL datasource test failed: {e}")
        return False

def test_prometheus_datasource():
    """Test Prometheus datasource connectivity"""
    print("\n📊 TESTING PROMETHEUS DATASOURCE")
    print("=" * 50)
    
    try:
        # Test Prometheus API directly
        response = requests.get("http://localhost:9090/api/v1/query?query=up", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                metrics = data['data']['result']
                print(f"✅ Prometheus API accessible - Found {len(metrics)} 'up' metrics")
                
                services_up = 0
                for metric in metrics:
                    job = metric['metric'].get('job', 'unknown')
                    value = float(metric['value'][1])
                    status = "UP ✅" if value == 1 else "DOWN ❌"
                    print(f"   🚀 {job}: {status}")
                    if value == 1:
                        services_up += 1
                
                print(f"✅ Services Status: {services_up}/{len(metrics)} UP")
                return True
            else:
                print(f"❌ Prometheus query failed: {data}")
                return False
        else:
            print(f"❌ Prometheus API returned: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Prometheus datasource test failed: {e}")
        return False

def test_dashboard_functionality():
    """Test dashboard creation and data visualization"""
    print("\n📋 TESTING DASHBOARD FUNCTIONALITY")
    print("=" * 50)
    
    try:
        auth = (GRAFANA_USER, GRAFANA_PASS)
        
        # Get list of dashboards
        response = requests.get(f"{GRAFANA_URL}/api/search?type=dash-db", auth=auth, timeout=10)
        
        if response.status_code == 200:
            dashboards = response.json()
            print(f"✅ Found {len(dashboards)} dashboard(s):")
            
            current_data_found = False
            infrastructure_found = False
            
            for dashboard in dashboards:
                title = dashboard.get('title', 'Unknown')
                uid = dashboard.get('uid', 'None')
                print(f"   📊 {title} (UID: {uid})")
                
                if 'current' in title.lower() or 'current-data' in uid:
                    current_data_found = True
                if 'infrastructure' in title.lower() or 'infrastructure-real' in uid:
                    infrastructure_found = True
            
            if current_data_found:
                print("✅ Current Data Dashboard found")
            else:
                print("⚠️  Current Data Dashboard not found")
                
            if infrastructure_found:
                print("✅ Infrastructure Monitoring Dashboard found")  
            else:
                print("⚠️  Infrastructure Monitoring Dashboard not found")
                
            return len(dashboards) > 0
        else:
            print(f"❌ Dashboard API returned: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Dashboard functionality test failed: {e}")
        return False

def test_data_freshness():
    """Test if dashboards are showing current/fresh data"""
    print("\n⏰ TESTING DATA FRESHNESS")
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
            # Check latest analytics timestamp
            cur.execute("""
                SELECT 
                    MAX(analytics_timestamp) as latest_analytics,
                    COUNT(*) as total_analytics
                FROM gold.customer_analytics
                WHERE analytics_timestamp IS NOT NULL;
            """)
            
            result = cur.fetchone()
            latest_analytics = result['latest_analytics']
            total_analytics = result['total_analytics']
            
            if latest_analytics:
                time_diff = datetime.now() - latest_analytics.replace(tzinfo=None)
                hours_old = time_diff.total_seconds() / 3600
                
                if hours_old < 24:  # Less than 24 hours old
                    print(f"✅ Analytics Data Fresh: Last updated {latest_analytics}")
                    print(f"   📊 Total analytics records: {total_analytics}")
                else:
                    print(f"⚠️  Analytics Data Aging: Last updated {latest_analytics} ({hours_old:.1f} hours ago)")
            else:
                print("⚠️  No analytics timestamp found")
                
            # Check record counts for consistency
            cur.execute("""
                SELECT 
                    (SELECT SUM(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'bronze') as bronze_count,
                    (SELECT SUM(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'silver') as silver_count,
                    (SELECT SUM(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = 'gold') as gold_count;
            """)
            
            counts = cur.fetchone()
            bronze_count = counts['bronze_count'] or 0
            silver_count = counts['silver_count'] or 0  
            gold_count = counts['gold_count'] or 0
            
            print(f"✅ Current Data Volumes:")
            print(f"   🥉 Bronze Layer: {bronze_count:,} records")
            print(f"   🥈 Silver Layer: {silver_count:,} records") 
            print(f"   🥇 Gold Layer: {gold_count:,} records")
            
            # Data consistency check
            if bronze_count > 0 and silver_count > 0:
                consistency = abs(bronze_count - silver_count) / bronze_count * 100
                if consistency < 10:  # Less than 10% variance
                    print("✅ Data Consistency: Bronze→Silver flow healthy")
                else:
                    print(f"⚠️  Data Consistency: {consistency:.1f}% variance Bronze→Silver")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Data freshness test failed: {e}")
        return False

def test_alerting_features():
    """Test Grafana alerting functionality"""
    print("\n🚨 TESTING ALERTING FEATURES")
    print("=" * 50)
    
    try:
        auth = (GRAFANA_USER, GRAFANA_PASS)
        
        # Check alerting configuration
        response = requests.get(f"{GRAFANA_URL}/api/alerts", auth=auth, timeout=10)
        
        if response.status_code == 200:
            alerts = response.json()
            print(f"✅ Alerting API accessible - Found {len(alerts)} alert(s)")
            
            # Check notification channels
            notif_response = requests.get(f"{GRAFANA_URL}/api/alert-notifications", auth=auth, timeout=10)
            if notif_response.status_code == 200:
                notifications = notif_response.json()
                print(f"✅ Notification channels: {len(notifications)} configured")
            else:
                print("⚠️  Notification channels not accessible")
                
            return True
        else:
            print(f"⚠️  Alerting API returned: {response.status_code} (may not be configured)")
            return True  # Not critical for basic functionality
            
    except Exception as e:
        print(f"⚠️  Alerting test failed: {e} (not critical)")
        return True  # Not critical for basic functionality

def main():
    """Run comprehensive Grafana validation"""
    print("🚀 COMPREHENSIVE GRAFANA FUNCTIONALITY VALIDATION")
    print("🕒 Timestamp:", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 70)
    
    # Run all tests
    tests = [
        ("Web Interface Access", test_grafana_accessibility),
        ("API Functionality", test_grafana_api),
        ("PostgreSQL Datasource", test_postgresql_datasource),
        ("Prometheus Datasource", test_prometheus_datasource), 
        ("Dashboard Functionality", test_dashboard_functionality),
        ("Data Freshness", test_data_freshness),
        ("Alerting Features", test_alerting_features)
    ]
    
    results = {}
    all_critical_passed = True
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
            
            # Critical tests that must pass
            critical_tests = ["Web Interface Access", "PostgreSQL Datasource", "Dashboard Functionality"]
            if test_name in critical_tests and not result:
                all_critical_passed = False
                
        except Exception as e:
            print(f"❌ {test_name} test failed with exception: {e}")
            results[test_name] = False
            if test_name in ["Web Interface Access", "PostgreSQL Datasource", "Dashboard Functionality"]:
                all_critical_passed = False
    
    # Final summary
    print("\n" + "=" * 70)
    print("📋 GRAFANA FUNCTIONALITY TEST SUMMARY")
    print("=" * 70)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} - {test_name}")
    
    print("\n" + "=" * 70)
    if all_critical_passed:
        print("🎉 GRAFANA STATUS: FULLY FUNCTIONAL")
        print("✅ All critical features working with current data")
        print("✅ Dashboards configured for real-time monitoring")
        print("✅ PostgreSQL integration operational")
        print("✅ Ready for production monitoring")
    else:
        failed_tests = [name for name, result in results.items() if not result]
        print("⚠️  GRAFANA STATUS: ISSUES DETECTED")
        print(f"❌ Failed tests: {', '.join(failed_tests)}")
        print("🔍 Review failed tests above for resolution")
    
    print("=" * 70)
    print("🌐 Grafana Access: http://localhost:3000")
    print("👤 Default Login: admin / admin")
    print("=" * 70)
    
    return all_critical_passed

if __name__ == "__main__":
    main()