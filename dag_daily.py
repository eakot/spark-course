from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

date = '{{ ds }}'

with DAG(
    dag_id='dag_by_Bogdan_v2',
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    tags=['lecture 7'],
    params={
        'example_key': 'example_value'
    }
) as dag:

    download = BashOperator(
        task_id='download_from_link',
        bash_command='cd /data; \
        if test -f {{ ds }};\
        then echo "file already exist";\
        else wget http://37.139.43.86/events/{{ ds }};\
        fi'
    )

    spark_test_task = SparkSubmitOperator(
        task_id='agg_to_parq',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/test.py',
        name='spark_test_task_app',
        execution_timeout=timedelta(minutes=5),
        application_args=[date]
    )

    download >> spark_test_task
