from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as f

spark = (SparkSession.builder
    .appName('test_postgres_connection')    
    .getOrCreate()
)

df = (spark.read.
    parquet('/data/bank.parquet')
)

(df
    .withColumn('load_datetime', f.lit(datetime.now()))
    
    .write
    .format('jdbc')
    .option('driver', 'org.postgresql.Driver')
    .mode('overwrite')
    .option('url', 'jdbc:postgresql://host.docker.internal:5432/postgres')
    .option("user", "p_user")
    .option("password", "password123")
    .option("dbtable", "public.bank")
    .option("fetchsize", 10000)
    .save()
)

spark.stop()
