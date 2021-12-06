from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from src.conf import SRC_FILE, TARGET_TABLE

spark = SparkSession.builder \
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

url = "jdbc:postgresql://postgresql:5432/postgres"

# spark aggregation
data = spark \
    .read\
    .jdbc(url=url,
    table=SRC_TABLE,
    properties=properties)

data.groupBy("age").agg(count("age").alias("num_of_clients")).orderBy("age").explain()

#sql aggregation
sql_query = "(SELECT age, COUNT(*) AS num_of_clients FROM {} GROUP BY age ORDER BY AGE) sql_agg".format(SRC_TABLE)
agg_by_age = spark \
    .read\
    .jdbc(url=url,
    table=sql_query,
    properties=properties)

agg_by_age.explain()

agg_by_age \
    .repartition(1) \
    .write.mode('overwrite') \
    .format('csv') \
    .save(TARGET_FILE, header=True)
