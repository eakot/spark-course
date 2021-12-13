import sys
from pyspark.sql import SparkSession


def load_to_db(df, table):
    (
        df
            .write
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .option("url", "jdbc:postgresql://172.17.0.1:5432/postgres")
            .option("user", "p_user")
            .option("password", "password123")
            .option("dbtable", table)
            .option("fetchsize", 10000)
            .save()
    )


load_data_date = sys.argv[1]
spark = SparkSession.builder \
    .getOrCreate()

df_vc = spark.read.parquet(f"/data/{load_data_date}_views_count.parquet")
table_vc = 'public.views_count'
load_to_db(df_vc, table_vc)

df_pc = spark.read.parquet(f"/data/{load_data_date}_purchase_count.parquet")
table_pc = 'public.purchase_count'
load_to_db(df_pc, table_pc)

spark.stop()
