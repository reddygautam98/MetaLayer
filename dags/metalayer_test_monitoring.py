from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import time

default_args = {
    'owner': 'metalayer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metalayer_test_monitoring',
    default_args=default_args,
    description='Simple test DAG for MetaLayer monitoring',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'monitoring', 'metalayer']
)

def generate_load():
    """Generate some CPU load for monitoring"""
    print("🚀 Starting MetaLayer test workload...")
    
    # Simulate data processing
    result = sum(i * i for i in range(50000))
    
    # Simulate database work
    time.sleep(2)
    
    print(f"✅ MetaLayer test completed. Result: {result}")
    return result

def simulate_data_quality_check():
    """Simulate data quality metrics"""
    import random
    
    print("🔍 Running data quality checks...")
    
    # Simulate quality score calculation
    quality_score = random.uniform(0.85, 0.99)
    records_processed = random.randint(1000, 5000)
    
    print(f"📊 Quality Score: {quality_score:.3f}")
    print(f"📈 Records Processed: {records_processed}")
    
    # Simulate some processing time
    time.sleep(1)
    
    return {"quality_score": quality_score, "records_processed": records_processed}

# Task 1: Generate system load
load_task = PythonOperator(
    task_id='generate_system_load',
    python_callable=generate_load,
    dag=dag
)

# Task 2: System info
system_info_task = BashOperator(
    task_id='system_info',
    bash_command='echo "🖥️ System Info: $(date) - $(hostname) - $(uptime)"',
    dag=dag
)

# Task 3: Data quality simulation
quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=simulate_data_quality_check,
    dag=dag
)

# Task 4: Final summary
summary_task = BashOperator(
    task_id='monitoring_summary',
    bash_command='echo "✅ MetaLayer monitoring test completed successfully at $(date)"',
    dag=dag
)

# Set task dependencies
load_task >> system_info_task >> quality_task >> summary_task