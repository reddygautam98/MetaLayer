"""
Simple Test DAG - Guaranteed to Work
This DAG uses only basic Airflow operators to ensure it loads properly.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Basic DAG configuration
default_args = {
    'owner': 'metalayer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# Create the DAG
dag = DAG(
    'simple_test_dag',
    default_args=default_args,
    description='Simple test DAG for MetaLayer',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'simple']
)

def hello_world():
    """Simple Python function"""
    print("🚀 Hello from MetaLayer!")
    print("✅ DAG is working correctly!")
    return "success"

def generate_metrics():
    """Generate some test metrics"""
    import random
    import time
    
    print("📊 Generating test metrics...")
    
    # Simulate some processing
    for i in range(5):
        value = random.randint(100, 1000)
        print(f"Metric {i+1}: {value}")
        time.sleep(0.5)
    
    print("✅ Metrics generation complete!")
    return "metrics_generated"

# Define tasks
hello_task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=dag
)

system_info = BashOperator(
    task_id='system_info',
    bash_command='echo "System: $(hostname) at $(date)"',
    dag=dag
)

metrics_task = PythonOperator(
    task_id='generate_metrics',
    python_callable=generate_metrics,
    dag=dag
)

completion_task = BashOperator(
    task_id='completion',
    bash_command='echo "✅ Simple test DAG completed successfully!"',
    dag=dag
)

# Set up task dependencies
hello_task >> system_info >> metrics_task >> completion_task