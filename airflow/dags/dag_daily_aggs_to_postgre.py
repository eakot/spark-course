
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

postgres_driver_jar = "/jars/postgresql-42.3.1.jar"
load_date = '{{ ds }}'

with DAG(
    dag_id='homework_7',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['homework 7'],
    params={"example_key": "example_value"},
) as dag:
    download_events = BashOperator(
        task_id='download_events',
        bash_command=f'cd /data; wget http://37.139.43.86/events/{load_date}',
    )

    # noinspection PyStubPackagesCompatibility
    load_aggs_to_postgre = SparkSubmitOperator(
        task_id="load_aggs_to_postgre",
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/load_aggs_to_postgre.py',
        name='load_aggs_to_postgre_app',
        execution_timeout=timedelta(minutes=20),
        application_args=[f'--load_date={load_date}',],
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar
    )
    download_events >> load_aggs_to_postgre