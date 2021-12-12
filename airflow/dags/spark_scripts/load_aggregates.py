import fire
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def fetch_to_db(df, dbtable):
    df \
        .withColumn('date_to_day', f.current_date()) \
        .write.format('jdbc') \
        .mode('append') \
        .option('driver', 'org.postresql.Driver') \
        .option('url', 'jdbc:postgresql://host.docker.internal:5432/postgres') \
        .option('user', 'p_user') \
        .option('password', 'password123') \
        .option('dbtable', dbtable) \
        .option('fetchsize', 10000) \
        .save()


def main(current_date):
    SCHEMA = 'date STRING, event_type STRING, product_id LONG, category_id LONG, ' \
             'category_code STRING, brand STRING, price FLOAT, user_id lONG, user_session STRING'

    SPARK = SparkSession.builder.getOrCreate()

    events_df = SPARK \
        .read \
        .option('sep', ',') \
        .option('header', 'false') \
        .option('enforceSchema', 'true') \
        .schema(SCHEMA) \
        .csv('/data/{}'.format(current_date))

    views_by_category = events_df \
        .where("event_type=='view'") \
        .groupby('category_code') \
        .count() \
        .orderBy('count', ascending=False)

    purchase_by_brand = events_df \
        .where('event_type=="purchase"') \
        .withColumn('rounded_price', f.round(events_df['price'], -1)) \
        .groupby('rounded_price') \
        .count() \
        .orderBy('rounded_price')

    fetch_to_db(views_by_category, 'public.views_by_category')
    fetch_to_db(purchase_by_brand, 'public.purchase_by_brand')

    SPARK.stop()


if __name__ == '__main__':
    fire.Fire(main)
