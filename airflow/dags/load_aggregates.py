
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='load_daily_aggregates',
    schedule_interval='@daily',
    start_date=datetime(2021, 12, 1),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['hw 7'],
    params={"example_key": "example_value"},
) as dag:

    download_data = BashOperator(
        task_id='download_data',
        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}',
    )

    load_aggregates = SparkSubmitOperator(
        task_id='load_aggs',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/load_aggregates.py',
        name='load_aggregates',
        execution_timeout=timedelta(minutes=20),
        application_args=['--load_date={{ ds }}', ],
        jars='/jars/postgresql-42.3.1.jar',
        driver_class_path='/jars/postgresql-42.3.1.jar'
    )
    download_data >> load_aggregates