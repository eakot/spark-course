from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName('spark_test_task')
    .getOrCreate()
)

print('\n\n\nSpark Configuration:\n')
for k, v in spark.sparkContext.getConf().getAll():
    print(f'{k}:        {v}')
print('\n\n\n')

spark.stop()
