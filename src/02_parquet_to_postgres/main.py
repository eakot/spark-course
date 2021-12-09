import fire
from pyspark.sql import SparkSession


def parquet_to_postgres(source_parquet_dir: str, target_tablename: str,
                        url: str, login: str, password: str) -> None:
    """
    Second Spark's task (except for downloading data) which reads
    the parquet file and writes the data to postgres db.
    """
    jars_path = 'jars/postgresql-42.3.1.jar'

    spark = (SparkSession.builder
        .config('spark.jars', jars_path)
        .appName('parquet_to_postgres')
        .getOrCreate()
    )

    # Extract
    df = (spark.read
        .parquet(source_parquet_dir)
        .na.drop('any')
    )

    # Load
    (df.write
        .format('jdbc')
        .mode('append')
        .option('driver', 'org.postgresql.Driver')
        .option('url', url)
        .option('user', login)
        .option('password', password)
        .option('dbtable', target_tablename)
        .option('fetchsize', 10000)
        .save()
    )
    
    spark.stop()


if __name__ == '__main__':
    fire.Fire(parquet_to_postgres)
