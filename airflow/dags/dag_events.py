from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='load_and_aggregation_bank_events',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['homework']
) as dag:

    load_date = '{{ ds }}'
    download = BashOperator(
        task_id='load_events',
        bash_command=f'cd /data; wget -O events_{load_date}.csv http://37.139.43.86/events/{load_date}'
    )

    spark_aggregation = SparkSubmitOperator(
        task_id='spark_aggregation',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/agg.py',
        application_args=[load_date],
        execution_timeout=timedelta(minutes=2),
        packages='org.postgresql:postgresql:42.2.24'
    )

download >> spark_aggregation