#!/usr/bin/env python3
"""
Airflow DAG Issue Diagnosis and Fix Script
Identifies and resolves stuck DAG runs and task instances
"""

import subprocess
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_docker_command(command):
    """Execute docker command and return output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}")
        logger.error(f"Error: {e.stderr}")
        return None

def clear_all_dag_runs():
    """Clear all stuck DAG runs"""
    logger.info("🧹 Clearing all stuck DAG runs")
    
    dags = [
        "master_etl_orchestrator",
        "bronze_layer_etl_pipeline", 
        "silver_layer_etl_pipeline",
        "gold_layer_analytics_pipeline"
    ]
    
    for dag in dags:
        logger.info(f"   Clearing {dag}")
        
        # Clear tasks for the last few days
        clear_cmd = f'docker exec etl_airflow_webserver airflow tasks clear {dag} -s 2025-11-08 -e 2025-11-11 --yes'
        result = run_docker_command(clear_cmd)
        
        if result is not None:
            logger.info(f"   ✅ Cleared {dag}")
        else:
            logger.warning(f"   ⚠️  Failed to clear {dag}")

def check_dag_runs():
    """Check current status of all DAG runs"""
    logger.info("📊 Checking DAG run status")
    
    dags = [
        "master_etl_orchestrator",
        "bronze_layer_etl_pipeline",
        "silver_layer_etl_pipeline", 
        "gold_layer_analytics_pipeline"
    ]
    
    for dag in dags:
        logger.info(f"\n--- {dag} ---")
        list_cmd = f'docker exec etl_airflow_webserver airflow dags list-runs --dag-id {dag}'
        result = run_docker_command(list_cmd)
        
        if result:
            print(result)
        else:
            logger.error(f"Failed to get runs for {dag}")

def restart_airflow_scheduler():
    """Restart Airflow scheduler to clear any stuck processes"""
    logger.info("🔄 Restarting Airflow scheduler")
    
    restart_cmd = "docker restart etl_airflow_scheduler"
    result = run_docker_command(restart_cmd)
    
    if result is not None:
        logger.info("✅ Scheduler restarted successfully")
        # Wait for scheduler to come up
        time.sleep(10)
    else:
        logger.error("❌ Failed to restart scheduler")

def trigger_fresh_pipeline():
    """Trigger a fresh pipeline run"""
    logger.info("🚀 Triggering fresh pipeline run")
    
    # Trigger master orchestrator
    trigger_cmd = 'docker exec etl_airflow_webserver airflow dags trigger master_etl_orchestrator'
    result = run_docker_command(trigger_cmd)
    
    if result:
        logger.info("✅ Fresh pipeline triggered")
        print(result)
    else:
        logger.error("❌ Failed to trigger pipeline")

def wait_and_monitor(timeout=300):
    """Monitor DAG progress for specified timeout"""
    logger.info(f"⏱️  Monitoring pipeline progress (timeout: {timeout}s)")
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Check master orchestrator status
        status_cmd = 'docker exec etl_airflow_webserver airflow dags list-runs --dag-id master_etl_orchestrator'
        result = run_docker_command(status_cmd)
        
        if result:
            lines = result.split('\n')
            if len(lines) > 2:  # Header + separator + data
                latest_run = lines[2].split('|')
                if len(latest_run) > 2:
                    state = latest_run[2].strip()
                    logger.info(f"Latest master orchestrator state: {state}")
                    
                    if state == "success":
                        logger.info("🎉 Pipeline completed successfully!")
                        return True
                    elif state == "failed":
                        logger.error("❌ Pipeline failed!")
                        return False
        
        time.sleep(30)
    
    logger.warning("⚠️  Monitoring timeout reached")
    return False

def check_container_health():
    """Check health of Airflow containers"""
    logger.info("🏥 Checking container health")
    
    containers = [
        "etl_airflow_webserver",
        "etl_airflow_scheduler", 
        "etl_airflow_worker",
        "etl_postgres",
        "etl_redis"
    ]
    
    for container in containers:
        health_cmd = f'docker inspect --format="{{{{.State.Health.Status}}}}" {container}'
        health = run_docker_command(health_cmd)
        
        if health:
            logger.info(f"   {container}: {health}")
        else:
            # Try basic status check
            status_cmd = f'docker inspect --format="{{{{.State.Status}}}}" {container}'
            status = run_docker_command(status_cmd)
            logger.info(f"   {container}: {status if status else 'unknown'}")

def main():
    """Main diagnosis and fix procedure"""
    print("🔍 AIRFLOW DAG ISSUE DIAGNOSIS AND FIX")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # Step 1: Check container health
    check_container_health()
    
    # Step 2: Show current DAG status
    logger.info("\n📊 CURRENT DAG STATUS")
    check_dag_runs()
    
    # Step 3: Clear stuck runs
    logger.info("\n🧹 CLEARING STUCK RUNS")
    clear_all_dag_runs()
    
    # Step 4: Restart scheduler
    logger.info("\n🔄 RESTARTING SCHEDULER")
    restart_airflow_scheduler()
    
    # Step 5: Trigger fresh run
    logger.info("\n🚀 TRIGGERING FRESH PIPELINE")
    trigger_fresh_pipeline()
    
    # Step 6: Monitor progress
    logger.info("\n⏱️  MONITORING PROGRESS")
    success = wait_and_monitor(300)  # 5 minute timeout
    
    # Final status
    logger.info("\n📋 FINAL STATUS")
    check_dag_runs()
    
    if success:
        print("\n🎉 AIRFLOW ISSUE RESOLUTION: SUCCESS")
        print("✅ Pipeline is now running correctly")
    else:
        print("\n⚠️  AIRFLOW ISSUE RESOLUTION: NEEDS ATTENTION")
        print("❌ Manual intervention may be required")
        print("🔍 Check Airflow UI at http://localhost:8080")
    
    print("=" * 50)

if __name__ == "__main__":
    main()