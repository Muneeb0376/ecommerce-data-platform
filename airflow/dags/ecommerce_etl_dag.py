"""Daily ETL: runs the existing pipeline (extract -> validate -> clean -> load -> db checks)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "muneeb",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl",
    description="Price updates + FX rates ETL into PostgreSQL",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["ecommerce", "etl"],
) as dag:

    run_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command="cd /opt/airflow/project && python -m src.pipeline",
    )