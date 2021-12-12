
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(
    dag_id='_load_events_daily_v3',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['lecture 7'],
    # params={"example_key": "example_value"},
) as dag:

    download = BashOperator(
        task_id='run_after_loop',
        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}',
    )

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id='spark_local',
        application="/opt/airflow/dags/spark_scripts/lecture_aggregates.py",
        application_args=['--date={{ ds }}'],
    )

    download >> submit_job