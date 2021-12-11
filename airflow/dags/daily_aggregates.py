from datetime import datetime, time, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='daily_aggregates',
    schedule_interval='@daily',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    tags=['homework']
) as dag:
    
    download = BashOperator(
        task_id='download_data',
        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}'
    )

    aggregates = SparkSubmitOperator(
        task_id='aggregates',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/aggregates/aggregates.py',
        application_args=['--source_filename={{ ds }}'],
        name='aggregates_spark_app',
        execution_timeout=timedelta(minutes=10),
        packages='org.postgresql:postgresql:42.2.24'
    )

    download >> aggregates
    