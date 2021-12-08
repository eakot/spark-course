
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='_load_events_daily_v2',
    schedule_interval='0 20 * * *',
    # schedule_interval='@once',
    start_date=datetime(2021, 12, 4),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=['lecture 7'],
    params={"example_key": "example_value"},
) as dag:

    download = BashOperator(
        task_id='run_after_loop',
        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}',
    )

    download