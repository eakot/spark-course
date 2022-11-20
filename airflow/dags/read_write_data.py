from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='spark_read_write_data',
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    end_date=datetime(2021, 11, 30),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60)
) as dag:

    jars_path = 'jars/postgresql-42.3.1.jar'
    url = f"postgresql://172.20.0.2:5432/postgres"
    download = BashOperator(
        task_id='bash_read_data',
        bash_command='cd /data; wget http://89.208.196.213/events/{{ds}}'
    )

    spark_read_task = SparkSubmitOperator(
        task_id='spark_read_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/csv2parquet.py',
        application_args=["--source_csv_file=/data/{{ds}}",
                          "--target_parquet_dir=/data"],
        name='spark_read_task_app',
        execution_timeout=timedelta(minutes=20)
    )

    postgres_connection_task_view = SparkSubmitOperator(
        task_id='postgres_connection_view',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/postgres_connection.py',
        application_args=["--source_parquet_dir=/data/count_view_{{ds_nodash}}.parquet",
                          "--target_tablename={count_view}",
                            f"--url={url}",
                            f"--user='p_user'",
                            f"--password='password123'"],
        name='postgres_connection_app_view',
        execution_timeout=timedelta(minutes=10),
        packages='org.postgresql:postgresql:42.2.24'
    )

    postgres_connection_task_purchase = SparkSubmitOperator(
        task_id='postgres_connection_purchase',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/postgres_connection.py',
        application_args=["--source_parquet_dir=/data/count_purchase_{{ds_nodash}}.parquet",
                          "--target_tablename={purchase_view}",
                            f"--url={url}",
                            f"--user='p_user'",
                            f"--password='password123'"],
        name='postgres_connection_app_purchase',
        execution_timeout=timedelta(minutes=10),
        packages='org.postgresql:postgresql:42.2.24'
    )
    download >> spark_read_task >> postgres_connection_task_view
    spark_read_task >> postgres_connection_task_purchase
