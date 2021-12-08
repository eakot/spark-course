from pyspark.sql import SparkSession
from src.conf import TARGET_TABLE, SAVED_TABLE
from loguru import logger
from src.log_df import log_df

import pyspark.sql.functions as f
import textwrap as tw

# set log file
logger.add('/spark_task_homework_spark/logs/log.log')

# spark settings
spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

# connection url
url = "jdbc:postgresql://postgresql:5432/postgre"

# load query
df = spark.read\
    .option("driver", "org.postgresql.Driver")\
    .format("jdbc")\
    .option("url", url)\
    .option("user", "p_user")\
    .option("password", "password123")\
    .option("dbtable", TARGET_TABLE)\
    .option("fetchsize", 10000)\
    .load()

# group df
df_grouped = df\
.groupBy("age")\
.agg(
    f.count("*").alias("rows_amount")
    )
                             

log_data = log_df(df, "SPARK", "explain")
logger.info(log_data)

log_data = log_df(df, "SPARK", "schema")
logger.info(log_data)


# save to csv
df_grouped.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").save(SAVED_TABLE)
#df_grouped.write.csv(SAVED_TABLE)



