from pyspark.sql import SparkSession
from src.conf import SRC_FILE, TARGET_TABLE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

df = spark.read.parquet(SRC_FILE).na.drop('any')

df.show()
df.printSchema()

url = "jdbc:postgresql://postgresql:5432/postgre"

(
df
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

try:

    test = spark.read\
        .option("driver", "org.postgresql.Driver")\
        .format("jdbc")\
        .option("url", url)\
        .option("user", "p_user")\
        .option("password", "password123")\
        .option("dbtable", TARGET_TABLE)\
        .option("fetchsize", 10000)\
        .load()

    print("Downloaded data:")
    test.printSchema()
    test.show()

except:
    print("Table cannot be loaded!")

