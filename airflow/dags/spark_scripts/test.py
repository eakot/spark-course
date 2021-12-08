from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .getOrCreate()

print("\n\n\nSpark configuration:\n")
for k, v in spark.sparkContext.getConf().getAll():
    print(f"{k}:        {v}")
print("\n\n\n")

spark.stop()
