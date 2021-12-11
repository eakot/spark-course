
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

postgres_driver_jar = "/jars/postgresql-42.3.1.jar"

with DAG(
    dag_id='spark_test',
    schedule_interval='@once',
    start_date=datetime(2021, 12, 1),
) as dag:

    spark_test_task = SparkSubmitOperator(
        task_id='spark_test_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/test.py',
        name='spark_test_task_app',
        execution_timeout=timedelta(minutes=2)
    )

    # noinspection PyStubPackagesCompatibility
    test_postgres_connection_task = SparkSubmitOperator(
        task_id="test_postgres_connection",
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/test_postgres_connection.py',
        name='spark_test_task_app',
        execution_timeout=timedelta(minutes=20),
        #packages='org.postgresql:postgresql:42.3.1',
        jars=postgres_driver_jar,
        driver_class_path=postgres_driver_jar
    )

    spark_test_task >> test_postgres_connection_task