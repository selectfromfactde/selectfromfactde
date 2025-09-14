
"""
Airflow DAG to orchestrate: bronze -> silver -> gold (local calls)
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner":"de", "retries":1}

with DAG(
    dag_id="olist_local_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    tags=["olist","local","spark"]
) as dag:

    bronze = BashOperator(
        task_id="bronze",
        bash_command="cd /opt/project && python -m src.etl.bronze_to_silver --mode bronze"
    )

    silver = BashOperator(
        task_id="silver",
        bash_command="cd /opt/project && python -m src.etl.bronze_to_silver --mode silver"
    )

    gold = BashOperator(
        task_id="gold",
        bash_command="cd /opt/project && python -m src.etl.silver_to_gold"
    )

    bronze >> silver >> gold
