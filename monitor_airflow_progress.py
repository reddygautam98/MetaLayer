#!/usr/bin/env python3
"""
Real-time Airflow DAG Progress Monitor
Shows live progress of DAG execution
"""

import subprocess
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command):
    """Execute command and return output"""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        return None

def get_dag_status(dag_id):
    """Get current status of DAG runs"""
    cmd = f'docker exec etl_airflow_webserver airflow dags list-runs --dag-id {dag_id}'
    result = run_command(cmd)
    
    if result:
        lines = result.split('\n')
        if len(lines) > 2:
            # Find the first running DAG
            for line in lines[2:]:  # Skip header and separator
                parts = line.split('|')
                if len(parts) >= 3:
                    state = parts[2].strip()
                    if state == "running":
                        return parts[1].strip(), state  # run_id, state
    
    return None, None

def get_task_progress(dag_id, run_id):
    """Get task progress for a specific DAG run"""
    cmd = f'docker exec etl_airflow_webserver airflow tasks states-for-dag-run {dag_id} {run_id}'
    result = run_command(cmd)
    
    if result:
        lines = result.split('\n')
        tasks = {'success': 0, 'running': 0, 'queued': 0, 'failed': 0, 'up_for_retry': 0, 'total': 0}
        
        for line in lines[2:]:  # Skip header and separator
            parts = line.split('|')
            if len(parts) >= 4:
                state = parts[3].strip()
                tasks['total'] += 1
                
                if state == "success":
                    tasks['success'] += 1
                elif state == "running":
                    tasks['running'] += 1
                elif state == "queued":
                    tasks['queued'] += 1
                elif state == "failed":
                    tasks['failed'] += 1
                elif state == "up_for_retry":
                    tasks['up_for_retry'] += 1
        
        return tasks
    
    return None

def monitor_dags(duration_minutes=10):
    """Monitor DAGs for specified duration"""
    print(f"\n🔍 AIRFLOW DAG PROGRESS MONITOR")
    print("="*60)
    print(f"⏰ Monitoring for {duration_minutes} minutes")
    print("="*60)
    
    dags_to_monitor = [
        ("master_etl_orchestrator", "Master Orchestrator"),
        ("bronze_layer_etl_pipeline", "Bronze Layer"),
        ("silver_layer_etl_pipeline", "Silver Layer"),
        ("gold_layer_analytics_pipeline", "Gold Layer")
    ]
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    iteration = 0
    
    while time.time() < end_time:
        iteration += 1
        current_time = datetime.now().strftime('%H:%M:%S')
        
        print(f"\n📊 Status Update #{iteration} - {current_time}")
        print("-" * 50)
        
        progress_found = False
        
        for dag_id, dag_name in dags_to_monitor:
            run_id, state = get_dag_status(dag_id)
            
            if state == "running" and run_id:
                progress_found = True
                tasks = get_task_progress(dag_id, run_id)
                
                if tasks:
                    total = tasks['total']
                    success = tasks['success']
                    running = tasks['running']
                    queued = tasks['queued']
                    failed = tasks['failed']
                    retry = tasks['up_for_retry']
                    
                    progress_pct = (success / total * 100) if total > 0 else 0
                    
                    print(f"🔄 {dag_name}")
                    print(f"   Progress: {success}/{total} tasks ({progress_pct:.1f}%)")
                    print(f"   Status: ✅{success} 🔄{running} ⏳{queued} ⚠️{retry} ❌{failed}")
                else:
                    print(f"🔄 {dag_name}: Running (getting task details...)")
            elif state:
                print(f"⏸️  {dag_name}: {state}")
        
        if not progress_found:
            print("⏸️  No DAGs currently running")
        
        # Wait before next check
        time.sleep(30)
    
    print(f"\n⏰ Monitoring completed after {duration_minutes} minutes")
    
    # Final status report
    print(f"\n📋 FINAL STATUS REPORT")
    print("="*50)
    
    for dag_id, dag_name in dags_to_monitor:
        run_id, state = get_dag_status(dag_id)
        if state:
            print(f"{dag_name}: {state}")
        else:
            print(f"{dag_name}: No active runs")

def main():
    """Main monitoring function"""
    try:
        monitor_dags(duration_minutes=5)  # Monitor for 5 minutes
    except KeyboardInterrupt:
        print("\n\n⏹️  Monitoring stopped by user")
    except Exception as e:
        print(f"\n❌ Monitoring error: {e}")

if __name__ == "__main__":
    main()