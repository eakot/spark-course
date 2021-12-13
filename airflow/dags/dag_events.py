from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


load_data_date = '{{ ds }}'
path_to_data = '/data'

with DAG(
    dag_id='load_events',
    schedule_interval='@once',
    start_date=datetime(2021, 12, 1),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=['home work 7']
) as dag:

    download = BashOperator(
        task_id='download_data',
        bash_command='cd /data; \
                      if test -f {{ ds }}.csv; \
                      then echo "Attempt to download {{ ds }}.csv at time - %s" >> events_log.txt; \
                      else wget -O {{ ds }}.csv http://37.139.43.86/events/{{ ds }}; \
                      fi;' % (datetime.now().strftime("%d-%m-%Y %H:%M"))
    )

    agg_save_to_parquet = SparkSubmitOperator(
        task_id='agg_data_parquet',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/agg_events.py',
        name='spark_agg_events_app',
        execution_timeout=timedelta(minutes=10),
        application_args=[load_data_date]
    )

    parquet_to_db = SparkSubmitOperator(
        task_id="parquet_to_db",
        conn_id='spark_local',
        application=f'/opt/airflow/dags/spark_scripts/agg_to_db.py',
        name='spark_parquet_to_db_app',
        execution_timeout=timedelta(minutes=10),
        packages='org.postgresql:postgresql:42.2.24',
        application_args=[load_data_date]
    )

    clear = BashOperator(
        task_id="clear_space",
        bash_command=f"rm {path_to_data}/{load_data_date}.csv; "
                     f"rm -r {path_to_data}/{load_data_date}_views_count.parquet; "
                     f"rm -r {path_to_data}/{load_data_date}_purchase_count.parquet;"
    )

    download >> agg_save_to_parquet >> parquet_to_db >> clear
