from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import *
from airflow import DAG


PATH_TO_DRIVER = "/home/jars/driver.jar"
connection_props = {"user":"p_user", "password":"password123", "driver":"org.postgresql.Driver"}
connection_url = "jdbc:postgresql://172.17.0.1:5432/postgres"

spark = SparkSession.builder.config("spark.jars", PATH_TO_DRIVER).getOrCreate()


Schema = StructType([StructField("event_time", TimestampType(), True),
                     StructField("event_type", StringType(), True),
                     StructField("product_id", LongType(), True),
                     StructField("category_id", LongType(), True),
                     StructField("category_code", StringType(), True),
                     StructField("brand", StringType(), True),
                     StructField("price", DoubleType(), True),
                     StructField("user_id", LongType(), True),
                     StructField("user_session", StringType(), True),
                     StructField("date", DateType(), True) ])

with DAG(
    dag_id='download_and_write_to_db',
    schedule_interval='30 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    max_active_runs = 1,
    dagrun_timeout=timedelta(minutes=60),
    tags=['airflow_homework']
) as dag:


    download = BashOperator(
        task_id='download_events_data',
        bash_command='wget -O /data/events_{{ ds }}.csv http://37.139.43.86/events/{{ ds }}'
    )


    def write_to_files_and_db(**kwargs):

        logical_date = kwargs['logical_date'].to_date_string()

        df_day = spark.read \
            .option("sep", ",") \
            .option("header", "false") \
            .option("inferSchema", "false") \
            .schema(Schema) \
            .csv(f"/data/events_{logical_date}.csv")

        df_views = df_day[df_day.event_type == "view"]\
            .groupBy("date", "category_code")\
            .agg(count('*').alias("views_count"))

        df_views.repartition(1).write.mode("overwrite")\
            .format("parquet").save(f"/data/views_{logical_date}.parquet")

        df_views.write\
            .jdbc(url=connection_url, table="public.views_count", mode="append", properties=connection_props)

        df_purchases = df_day[df_day.event_type == "purchase"]\
            .groupBy("date", "brand")\
            .agg(count('*').alias("purchases_count"))

        df_purchases.repartition(1).write.mode("overwrite") \
            .format("parquet").save(f"/data/purchases_{logical_date}.parquet")

        df_purchases.write \
            .jdbc(url=connection_url, table="public.purchases_count", mode="append", properties=connection_props)

        return


    write_ops = PythonOperator(
        task_id='write_to_files_and_db',
        dag=dag,
        templates_dict={'logical_date' : '{{ ds }}'},
        python_callable=write_to_files_and_db
    )

    download >> write_ops
