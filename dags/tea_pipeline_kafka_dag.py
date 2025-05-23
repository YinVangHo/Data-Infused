from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="tea_pipeline_kafka_dag",
    default_args=default_args,
    description="Run Kafka tea producer and consumer to Snowflake",
    schedule_interval="@once",  # or "*/15 * * * *" for every 15 mins
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tea", "kafka", "snowflake"],
) as dag:

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command="python /opt/airflow/kafka/producer.py"
    )

    run_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command="python /opt/airflow/kafka/consumer.py"
    )

    run_producer >> run_consumer  # Set task order
