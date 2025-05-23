from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "yinvang",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="tea_dbt_transform",
    default_args=default_args,
    description="Run dbt models & tests on ingested tea data",
    schedule_interval="@daily",
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=["dbt", "transform"],
) as dag:

    # Wait for the ingest run from two hours prior to ensure data availability
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_latest_ingest",
        external_dag_id="tea_ingest",
        external_task_id="run_consumer",
        execution_delta=timedelta(hours=2),
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd /opt/airflow/data_infused_dbt && "
            "dbt seed --profiles-dir ."
        ),
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_01_staging",
        bash_command=(
            "cd /opt/airflow/data_infused_dbt && "
            "dbt run --models 01_staging --profiles-dir ."
        ),
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_02_intermediate",
        bash_command=(
            "cd /opt/airflow/data_infused_dbt && "
            "dbt run --models 02_intermediate --profiles-dir ."
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_03_marts",
        bash_command=(
            "cd /opt/airflow/data_infused_dbt && "
            "dbt run --models 03_marts --profiles-dir ."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/data_infused_dbt && "
            "dbt test --profiles-dir ."
        ),
    )

    # Chain tasks: wait_for_ingest -> seed -> staging -> intermediate -> marts -> test
    wait_for_ingest >> dbt_seed >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test
