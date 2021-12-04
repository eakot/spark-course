from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from src.conf import SRC_TABLE, TARGET_FILE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

bank_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgresql:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", SRC_TABLE) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .load()

age_df = bank_df\
            .groupBy("age") \
            .agg(f.count("*").alias("count")) \
            .orderBy("age")

age_df\
    .write \
    .mode('overwrite') \
    .option("sep", ";") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('data/age.csv')

bank_df\
    .groupBy("age") \
    .agg(f.count("*").alias("count")) \
    .orderBy("age") \
    .explain()

_select_sql = "(select age, count(*) as count from public.bank group by age order by age) emp_alias"

bank_pd_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgresql:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", _select_sql) \
    .option("user", "p_user") \
    .option("password", "password123") \
    .load() \
    .explain()



