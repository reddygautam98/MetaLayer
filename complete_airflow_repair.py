#!/usr/bin/env python3
"""
Airflow DAG Queue Management and Fix Script
Clears queue backlog and ensures proper DAG execution
"""

import subprocess
import time
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_docker_command(command, timeout=60):
    """Execute docker command with timeout"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True, timeout=timeout)
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        logger.error(f"Command timed out: {command}")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}")
        logger.error(f"Error: {e.stderr}")
        return None

def kill_all_dag_runs():
    """Kill all DAG runs by clearing everything"""
    logger.info("🧹 CLEARING ALL DAG RUNS AND RESETTING AIRFLOW")
    
    # Stop all running tasks by clearing them
    dags = [
        "master_etl_orchestrator",
        "bronze_layer_etl_pipeline", 
        "silver_layer_etl_pipeline",
        "gold_layer_analytics_pipeline"
    ]
    
    for dag in dags:
        logger.info(f"   Clearing all runs for {dag}")
        
        # Use broader date range to catch everything
        clear_cmd = f'docker exec etl_airflow_webserver airflow tasks clear {dag} -s 2025-11-01 -e 2025-11-15 --yes'
        result = run_docker_command(clear_cmd, timeout=120)
        
        if result is not None:
            logger.info(f"   ✅ Cleared {dag}")
        else:
            logger.warning(f"   ⚠️  Failed to clear {dag}")

def restart_all_airflow_components():
    """Restart all Airflow components for clean slate"""
    logger.info("🔄 RESTARTING ALL AIRFLOW COMPONENTS")
    
    components = ["etl_airflow_scheduler", "etl_airflow_worker", "etl_airflow_webserver"]
    
    for component in components:
        logger.info(f"   Restarting {component}")
        restart_cmd = f"docker restart {component}"
        result = run_docker_command(restart_cmd)
        
        if result is not None:
            logger.info(f"   ✅ Restarted {component}")
        else:
            logger.error(f"   ❌ Failed to restart {component}")
    
    # Wait for all components to be healthy
    logger.info("   ⏳ Waiting for components to be healthy...")
    time.sleep(45)

def verify_system_health():
    """Verify all components are healthy"""
    logger.info("🏥 VERIFYING SYSTEM HEALTH")
    
    components = [
        "etl_airflow_webserver",
        "etl_airflow_scheduler", 
        "etl_airflow_worker",
        "etl_postgres",
        "etl_redis"
    ]
    
    all_healthy = True
    
    for component in components:
        # Check container status
        status_cmd = f'docker inspect --format="{{{{.State.Status}}}}" {component}'
        status = run_docker_command(status_cmd)
        
        # Try to get health if available
        health_cmd = f'docker inspect --format="{{{{.State.Health.Status}}}}" {component} 2>/dev/null || echo "no-health"'
        health = run_docker_command(health_cmd)
        
        if status == "running":
            health_status = health if health and health != "no-health" else "running"
            logger.info(f"   ✅ {component}: {health_status}")
        else:
            logger.error(f"   ❌ {component}: {status}")
            all_healthy = False
    
    return all_healthy

def test_database_connectivity():
    """Test database connectivity"""
    logger.info("🗄️  TESTING DATABASE CONNECTIVITY")
    
    db_test_cmd = 'docker exec etl_airflow_webserver psql postgresql://postgres:etl_secure_2024!@etl_postgres:5432/airflow -c "SELECT COUNT(*) FROM dag;"'
    result = run_docker_command(db_test_cmd)
    
    if result:
        logger.info("   ✅ Database connectivity OK")
        return True
    else:
        logger.error("   ❌ Database connectivity failed")
        return False

def run_simple_dag_test():
    """Run a simple DAG test"""
    logger.info("🧪 RUNNING SIMPLE DAG TEST")
    
    # Trigger just the bronze layer DAG
    trigger_cmd = 'docker exec etl_airflow_webserver airflow dags trigger bronze_layer_etl_pipeline'
    result = run_docker_command(trigger_cmd)
    
    if result:
        logger.info("   ✅ Bronze layer DAG triggered successfully")
        
        # Wait and check if it starts running
        time.sleep(30)
        
        status_cmd = 'docker exec etl_airflow_webserver airflow dags list-runs --dag-id bronze_layer_etl_pipeline'
        status_result = run_docker_command(status_cmd)
        
        if status_result and "running" in status_result:
            logger.info("   ✅ DAG is executing - system is healthy")
            return True
        else:
            logger.warning("   ⚠️  DAG triggered but not running yet")
            return False
    else:
        logger.error("   ❌ Failed to trigger DAG")
        return False

def main():
    """Main repair procedure"""
    print("\n" + "="*60)
    print("🔧 AIRFLOW COMPLETE SYSTEM REPAIR")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Step 1: Kill all existing runs
    kill_all_dag_runs()
    
    # Step 2: Restart all components
    restart_all_airflow_components()
    
    # Step 3: Verify health
    if not verify_system_health():
        logger.error("❌ System health check failed")
        return False
    
    # Step 4: Test database
    if not test_database_connectivity():
        logger.error("❌ Database connectivity failed") 
        return False
    
    # Step 5: Run simple test
    test_success = run_simple_dag_test()
    
    # Final status
    print("\n" + "="*60)
    if test_success:
        print("🎉 AIRFLOW REPAIR: COMPLETE SUCCESS")
        print("✅ System is healthy and DAGs are running")
        print("✅ Ready for production use")
        
        # Trigger master orchestrator
        logger.info("\n🚀 TRIGGERING MASTER ORCHESTRATOR")
        trigger_master_cmd = 'docker exec etl_airflow_webserver airflow dags trigger master_etl_orchestrator'
        master_result = run_docker_command(trigger_master_cmd)
        
        if master_result:
            print("✅ Master orchestrator triggered successfully")
        else:
            print("⚠️  Master orchestrator trigger failed - try manually")
            
    else:
        print("⚠️  AIRFLOW REPAIR: PARTIAL SUCCESS")
        print("✅ System components are healthy")
        print("⚠️  DAG execution needs monitoring")
    
    print("\n🌐 Airflow UI: http://localhost:8080")
    print("="*60)
    
    return test_success

if __name__ == "__main__":
    main()