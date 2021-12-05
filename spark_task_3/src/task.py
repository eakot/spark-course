from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from src.conf import SRC_TABLE, TARGET_FILE

spark = SparkSession.builder\
    .config("spark.driver.memory", "2G") \
    .config("spark.jars", "jars/postgresql-42.3.1.jar") \
    .getOrCreate()

properties = {'user' : 'p_user',
         'password' : 'password123',
         'driver' : 'org.postgresql.Driver'}

url = 'jdbc:postgresql://postgresql:5432/postgres'

spark_data = spark.read.jdbc(url=url,
                     table=SRC_TABLE,
                     properties=properties)

# pyspark version
grouped_by_age = spark_data.groupBy('age')\
    .agg(f.count('*').alias('clients_count'))\
    .orderBy('age')

print('PYSPARK PHYSICAL PLAN: ')
grouped_by_age.explain()

# sql query version
query = '(SELECT age, COUNT(*) AS clients_count FROM {} GROUP BY age ORDER BY AGE) sql_agg'.format(SRC_TABLE)
sql_data = spark.read.jdbc(url=url,
                           table=query,
                           properties=properties)

print('SQL QUERY PHYSICAL PLAN: ')
sql_data.explain()

grouped_by_age\
    .repartition(1)\
    .write.mode('overwrite')\
    .format('csv')\
    .save(TARGET_FILE, header=True)