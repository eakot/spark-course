from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='spark_test_db',
    schedule_interval='@once',
    start_date=datetime(2021, 12, 1),
) as dag:

    spark_test_db_task = SparkSubmitOperator(
        task_id='spark_test_db_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/test_db.py',
        name='spark_test_db_task_app',
        execution_timeout=timedelta(minutes=2),
        dag=dag
    )

    spark_test_db_task