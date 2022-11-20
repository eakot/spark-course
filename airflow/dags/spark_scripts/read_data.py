from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName('postgres_connection_app') \
    .getOrCreate()
# Prepare_data
data_schema = [
    StructField('event_time', TimestampType(), True),
    StructField('event_type', StringType(), True),
    StructField('product_id', IntegerType(), True),
    StructField('category_id', LongType(), True),
    StructField('category_code', StringType(), True),
    StructField('brand', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('user_id', IntegerType(), True),
    StructField('user_session', StringType(), True),
    StructField('date', DateType(), True)
]
final_struct = StructType(fields=data_schema)

# Extract
df = spark.read \
     .option('sep', ',') \
     .csv("/data/2021-11-26")

# Transform
count_view = df.filter(f.col('_c1') == 'view').count()
count_purchase = df.filter(f.col('_c1') == 'purchase').count()
df_load = spark.createDataFrame([{"Count_view": count_view, "Count_purchase": count_purchase}])

# Load
# (df_load
#    .repartition(1)
#    .write
#    .mode('append')
#    .format('csv')
#    .save("/data/2021-11-26.csv"))

(
    df_load
        .write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .option("url", "jdbc:postgresql://172.20.0.2:5432/postgres")
        .option("user", "p_user")
        .option("password", "password123")
        .option("dbtable", "public.events")
        .save()
)

spark.stop()
