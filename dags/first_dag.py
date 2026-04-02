"""
Тестовый даг
"""
from airflow import DAG
from datetime import datetime
import logging

from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    'start_date': datetime(2024, 1, 1),
    'owner': 'ds'
}

with DAG(
    "ds_test",
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['ds']
) as dag:

    dummy = EmptyOperator(task_id="dummy")

    echo_ds = BashOperator(
        task_id='echo_ds',
        bash_command='echo {{ ds }}'
    )

    def hello_world_func():
        logging.info("Hello World!")

    hello_world = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world_func
    )

    dummy >> [echo_ds, hello_world]