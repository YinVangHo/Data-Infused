from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'yin_vang',                         # For tracking DAG ownership
    'retries': 1,                                # Retry once if a task fails
    'retry_delay': timedelta(minutes=2),         # Wait 5 minutes before retrying
    'start_date': datetime(2024, 4, 1),          # DAG won't run before this date
}

# Simple function to run any Python script using subprocess
def run_script(path):
    subprocess.run(["python", path], check=True)

# Define the DAG
with DAG(
    dag_id="tea_pipeline_dag",                 
    default_args=default_args,
    schedule_interval=None,                     
    catchup=False,                              
    tags=["capstone"]                           
) as dag:

    # Step 1: Extract batch data from source
    extract = PythonOperator(
        task_id="extract_batch",
        python_callable=lambda: run_script("src/extract_batch.py"),
    )

    # Step 2: Load cleaned data into Snowflake
    load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=lambda: run_script("src/load_tea_data.py"),
    )

    # Step 3: Transform the raw table into analytics-ready format
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=lambda: run_script("src/transform_tea_data.py"),
    )

    # Task dependencies: extract → load → transform
    extract >> load >> transform
