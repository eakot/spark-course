from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_FILE
from pyspark.sql.functions import count

spark = SparkSession.builder \
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

df_agg = spark \
    .read \
    .option("driver", "org.postgresql.Driver") \
    .format("jdbc") \
    .option("url", url) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .option("dbtable", TARGET_FILE) \
    .option("fetchsize", 10000) \
    .load()

df_agg.groupBy("age").agg(count("*").alias("count")).orderBy("age").explain()

pushdown_sql = "(select age, count(*) as count from bank group by age order by age) count_age"

pushdown_df = spark \
    .read \
    .option("driver", "org.postgresql.Driver") \
    .format("jdbc") \
    .option("url", url) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .option("dbtable", pushdown_sql) \
    .option("fetchsize", 10000) \
    .load()

pushdown_df.explain()
(
    pushdown_df
    .write
    .option("header", "true")
    .option("sep", ";")
    .mode("overwrite")
    .csv(SRC_FILE)
)
