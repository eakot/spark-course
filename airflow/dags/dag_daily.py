from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='_load_events_daily_v2',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['lecture 7'],
    params={"example_key": "example_value"},
) as dag:

    load_data = "{{ ds }}"
    download_task = BashOperator(
    task_id='run_after_loop',
    bash_command=f'cd /data; wget http://37.139.43.86/events/{load_data}',
)

spark_test_task = SparkSubmitOperator(
    task_id='spark_test_task_hw',
    conn_id='spark_local',
    application=f'/opt/airflow/dags/spark_scripts/hw_task.py --load_data={load_data}',
    name='spark_test_task_app_hw',
    execution_timeout=timedelta(minutes=2)
)

download_task >> spark_test_task