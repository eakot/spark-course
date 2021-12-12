from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='load_events_daily_hw5',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    max_active_runs = 1,
    dagrun_timeout=timedelta(minutes=60),
) as dag:

    load_date = '{{ ds }}'

    download_task = BashOperator(
        task_id='load_frame',
        bash_command=f'cd /data; wget -O events_{load_date}.csv http://37.139.43.86/events/{load_date}'
    )

    add_agg_task = SparkSubmitOperator(
        task_id='add_agg_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/agg_for_events.py',
        application_args=[load_date],
        execution_timeout=timedelta(minutes=2),
        packages='org.postgresql:postgresql:42.2.24'
    )

download_task >> add_agg_task