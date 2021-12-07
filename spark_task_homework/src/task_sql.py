from pyspark.sql import SparkSession
from src.conf import TARGET_TABLE, SAVED_TABLE_SQL
from loguru import logger
from src.log_df import log_df

import pyspark.sql.functions as f
import textwrap as tw

# set log file
logger.add('/spark_task_homework_sql/logs/log.log')

# spark settings
spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

# connection url
url = "jdbc:postgresql://postgresql:5432/postgre"


# query
sql_query = """
(
    SELECT age,
           count(*) rows_amount
FROM {}
GROUP BY 1
) t
""".format(TARGET_TABLE)

# load query
df = spark.read\
    .option("driver", "org.postgresql.Driver")\
    .format("jdbc")\
    .option("url", url)\
    .option("user", "p_user")\
    .option("password", "password123")\
    .option("dbtable", sql_query)\
    .option("fetchsize", 10000)\
    .load()


log_data = log_df(df, "SQL", "explain")
logger.info(log_data)

log_data = log_df(df, "SQL", "schema")
logger.info(log_data)

# save to csv
df.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").save(SAVED_TABLE_SQL)
