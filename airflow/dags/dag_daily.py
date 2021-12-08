
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='_load_events_daily_v2',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 12, 1),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['lecture 7'],
    params={"example_key": "example_value"},
) as dag:

    download = BashOperator(
        task_id='run_after_loop',
        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}',
    )

    download