from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "yinvang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tea_ingest",
    default_args=default_args,
    description="Produce test messages and consume from Kafka into Snowflake staging",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["ingest", "kafka"],
) as dag:

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python3 /opt/airflow/kafka/producer.py --config /opt/airflow/config/kafka_config.yml",
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="python3 /opt/airflow/kafka/consumer.py --config /opt/airflow/config/kafka_config.yml",
    )

    # No dependencies – both run independently
