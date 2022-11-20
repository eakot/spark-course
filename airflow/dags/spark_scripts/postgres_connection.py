from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from datetime import datetime

spark = SparkSession.builder \
    .appName('postgres_connection_app') \
    .getOrCreate()

# write to airflow-postgres database
# .option("url", "jdbc:postgresql://airflow-postgres:5432/airflow")
# .option("user", "airflow")
# .option("password", "airflow")
# .option("dbtable", "public.aa_bank")
# On Linux change host.docker.internal  >> 172.17.0.1
# df = spark.read.parquet("/data/2021-11-26.parquet")


(
    df_load
        .write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres")
        .option("user", "p_user")
        .option("password", "password123")
        .option("dbtable", "public.events")
        .option("fetchsize", 10000)
        .save()
)

spark.stop()
