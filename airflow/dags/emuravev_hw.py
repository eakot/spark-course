import datetime as dt
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow import settings
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="emuravev_hw",
    schedule_interval="@daily",
    start_date=dt.datetime(year=2021, month=11, day=26),
    end_date=dt.datetime(year=2022, month=12, day=1),
    catchup=True,
) as dag:
    session = settings.Session()
    u = session.bind.url
    url = f"postgresql://{u.host}:5432/{u.database}"
    user = u.username
    password = u.password_original

    jars = '/data/jars/postgresql-42.5.0.jar'

    def _move_csv_to_error_callback(file):
        ...

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /data/events && "
            "curl -f -o /data/events/{{ds}}.csv "
            "http://89.208.196.213/events/{{ds}}"
        )
    )

    calculate_stats = SparkSubmitOperator(
        task_id='calculate_stats',
        conn_id='spark_local',
        application='/opt/airflow/dags/spark_scripts/csv_to_parquet.py',
        application_args=["--source_csv_file=/data/events/{{ds}}.csv",
                          "--target_parquet_dir=/data/events/stats"],
        name='calculate_stats_app',
        execution_timeout=dt.timedelta(minutes=2),
        on_failure_callback=_move_csv_to_error_callback
    )

    category_views_to_pg = SparkSubmitOperator(
        task_id='category_views_to_pg',
        conn_id='spark_local',
        application='/opt/airflow/dags/spark_scripts/parquet_to_bq.py',
        application_args=["--source_parquet_dir=/data/events/stats/category_views_{{ds_nodash}}.parquet",
                          "--target_tablename=category_views",
                          f"--url={url}",
                          f"--user={user}",
                          f"--password={password}",
                          ],
        name='category_views_to_pg_app',
        jars=jars,
        driver_class_path=jars,
        execution_timeout=dt.timedelta(minutes=2),
        on_failure_callback=_move_csv_to_error_callback
    )

    brand_purchases_to_pg = SparkSubmitOperator(
        task_id='brand_purchases_to_pg',
        conn_id='spark_local',
        application='/opt/airflow/dags/spark_scripts/parquet_to_bq.py',
        application_args=["--source_parquet_dir=/data/events/stats/brand_purchases_{{ds_nodash}}.parquet",
                          "--target_tablename=brand_purchases",
                          f"--url={url}",
                          f"--user={user}",
                          f"--password={password}",
                          ],
        name='brand_purchases_to_pg_app',
        jars=jars,
        driver_class_path=jars,
        execution_timeout=dt.timedelta(minutes=2),
        on_failure_callback=_move_csv_to_error_callback
    )

    move_csv_to_success = BashOperator(
        task_id='move_csv_to_success',
        bash_command=(
            "mkdir -p /data/events/csv_success && "
            "mv /data/events/{{ds}}.csv "
            "/data/events/csv_success/{{ ts_nodash }}_{{ds}}.csv"
        )
    )

    start_task = DummyOperator(task_id='start')
    final_task = DummyOperator(task_id='end')

    start_task >> fetch_events >> calculate_stats
    calculate_stats >> [category_views_to_pg, brand_purchases_to_pg] >> move_csv_to_success >> final_task

