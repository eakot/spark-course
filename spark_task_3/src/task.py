from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from src.conf import SRC_FILE, TARGET_TABLE

spark = SparkSession.builder \
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

pushdown_query = "(select age, count(*) as count from bank group by age order by age) count_age"

pushdown_df = spark \
    .read \
    .option("driver", "org.postgresql.Driver") \
    .format("jdbc") \
    .option("url", url) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .option("dbtable", pushdown_query) \
    .option("fetchsize", 10000) \
    .load()

pushdown_df.explain()

(
    pushdown_df
    .coalesce(1)
    .write
    .option("header", "true")
    .option("sep", ";")
    .mode("overwrite")
    .csv(SRC_FILE)
)

df_agg = spark \
    .read \
    .option("driver", "org.postgresql.Driver") \
    .format("jdbc") \
    .option("url", url) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .option("dbtable", TARGET_TABLE) \
    .option("fetchsize", 10000) \
    .load()

df_agg.groupBy("age").agg(count("age").alias("cnt")).orderBy("age").explain()

