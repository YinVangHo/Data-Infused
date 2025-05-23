from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for both producer and consumer tasks
default_args = {
    "owner": "yinvang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tea_ingest",
    default_args=default_args,
    description="Produce test messages and consume from Kafka into Snowflake staging",
    schedule_interval="@hourly",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["ingest", "kafka"],
) as dag:

    # 1) Produce synthetic test messages into Kafka
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            "python3 /opt/airflow/kafka/producer.py "
            "--config /opt/airflow/config/kafka_config.yml"
        ),
    )

    # 2) Consume from Kafka and load into Snowflake staging
    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command=(
            "python3 /opt/airflow/kafka/consumer.py "
            "--config /opt/airflow/config/kafka_config.yml"
        ),
    )

    # Ensure producer runs before consumer
    run_producer >> run_consumer
