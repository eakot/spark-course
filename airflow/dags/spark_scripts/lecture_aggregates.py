from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as f
import fire
from pyspark.sql.types import (
    StructField, StructType, LongType, StringType, DateType
)


def main(date):
    spark = SparkSession.builder.getOrCreate()
    url = "jdbc:postgresql://host.docker.internal:5432/postgres"
    # write to airflow-postgres database
    # .option("url", "jdbc:postgresql://airflow-postgres:5432/airflow")
    # .option("user", "airflow")
    # .option("password", "airflow")
    # .option("dbtable", "public.aa_bank")

    schema = StructType([
        StructField('event_time', DateType(), True),
        StructField('event_type', StringType(), True),
        StructField('product_id', LongType(), True),
        StructField('category_id', LongType(), True),
        StructField('category_code', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('price', LongType(), True),
        StructField('user_id', LongType(), True),
        StructField('user_session', StringType(), True),
        StructField('date', DateType(), True)
    ])
    # On Linux change host.docker.internal  >> 172.17.0.1
    df = (
        spark
            .read
            .format('csv')
            .schema(schema)
            .option('header', 'true')
            .option('delimiter', ',')
            .load("/data/{}".format(date))
    )
    # (
    #     df.withColumn("load_datetime", f.lit(datetime.now()))
    #     .write.format("jdbc")
    #     .option("driver", "org.postgresql.Driver")
    #     .mode("overwrite")
    #     .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres")
    #     .option("user", "p_user")
    #     .option("password", "password123")
    #     .option("dbtable", "public.bank")
    #     .option("fetchsize", 10000)
    #     .save()
    # )

    event_type_count = (
        df.groupBy("event_type")
        .agg(f.count("*").alias("count"))
        .orderBy(f.col("count").desc())
    )
    event_type_count.show()
    event_type_count.write.mode('overwrite').csv('/data/event_type_count_{}'.format(date))
    (
        event_type_count
            .write
            .format('jdbc')
            .option('driver', 'org.postgresql.Driver')
            .mode('append')
            .option('url', url)
            .option("user", "p_user")
            .option("password", "password123")
            .option('dbtable', 'event_type_count')
            .save()
     )
    category_top = (
        df.filter((df["event_type"] == "view") & (df["category_code"].isNotNull()))
        .groupBy("category_code")
        .agg(f.count("*").alias("view_count"))
        .orderBy(f.desc("view_count"))
    )
    category_top.show()
    category_top.write.mode('overwrite').csv('/data/category_top_{}'.format(date))

    # spark.stop()


if __name__ == '__main__':
    fire.Fire(main)