import airflow
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, LongType, DoubleType

DAGNAME = 'agg_data'

args = {
    'owner': 'airflow',
    #'start_date' : datetime(2021, 11, 26),
    #'catchup': True, 
    'provide_context': True,
    #'retries': 5,
    #'retry_delay': timedelta(minutes=5)
}


### DAGS
def agg_data(ds, **kwargs):

    # get filename - execution date
    filename = f'{kwargs["logical_date"].date()}.csv'
    filename_parquet = f'{kwargs["logical_date"].date()}'

    # create spark
    #spark = SparkSession.builder.config("spark.driver.memory", "2G").getOrCreate()
    spark = SparkSession.builder\
        .config("spark.driver.memory", "2G") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
        .getOrCreate()


    # schema for csv
    schema = StructType() \
          .add("event_time", DateType(),True) \
          .add("event_type", StringType(),True) \
          .add("product_id", IntegerType(),True) \
          .add("category_id", LongType(),True) \
          .add("category_code", StringType(),True) \
          .add("brand", StringType(),True) \
          .add("price", DoubleType(),True) \
          .add("user_id", IntegerType(),True) \
          .add("user_session", StringType(),True) \
          .add("date", DateType(),True)

    # read csv file and apply schema
    df = spark.read.format("csv") \
          .option("header", True) \
          .schema(schema) \
          .load(f"/data/{filename}")

    # количество просмотров по категориям (date, category, views_count)
    df_cat = df\
          .where("event_type == 'view'")\
          .groupBy("date", "category_code")\
          .agg(
               f.count("*").alias("views_count")
              )

    # продажи товаров брендов (date, brand, purchase_count)
    df_sale = df\
          .where("event_type == 'purchase'")\
          .groupBy("date", "brand")\
          .agg(
               f.count("*").alias("purchase_count")
              )

    # print schemas
    df.printSchema()
    df_cat.printSchema()
    df_sale.printSchema()

    # save to parquet
    df_cat.repartition(1).write.mode("overwrite").format("parquet").save(f"/data/{filename_parquet}_cat.parquet")
    df_sale.repartition(1).write.mode("overwrite").format("parquet").save(f"/data/{filename_parquet}_sale.parquet")
    
    # load to PostgreSQL
    url = "jdbc:postgresql://172.17.0.1:5432/postgre"
    TARGET_TABLE = "public.category_table"

    (
    df_cat
        .write
        .option("driver", "org.postgresql.Driver")
        .format("jdbc")
        .mode("append")
        .option("url", url)
        .option("user", "p_user")
        .option("password", "password123")
        .option("dbtable", TARGET_TABLE)
        .option("fetchsize", 10000)
        .save(TARGET_TABLE)
    )



dag = airflow.DAG(
    DAGNAME,
    schedule_interval='0 20 * * *',
    start_date=datetime(2021, 11, 26),
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    #default_args=args,
    tags=['homework'],
    max_active_runs=1,
)


download_data = BashOperator(
                        task_id='download_data',
                        bash_command='cd /data; wget http://37.139.43.86/events/{{ ds }}; mv {{ ds }} {{ ds }}.csv',
                        dag=dag
                       )


agg_data = PythonOperator(task_id='agg_data',
                    python_callable=agg_data,
                    #provide_context=False,
                    dag=dag)


### TASK QUEUE
download_data >> agg_data
