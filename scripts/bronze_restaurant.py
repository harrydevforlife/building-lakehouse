import os

from delta import *
from pyspark.sql import SparkSession


aws_access_key = 'minio_key'
aws_secret_key = 'minio_secret'

spark = SparkSession \
        .builder \
        .appName("Ingest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "s3a://prod/lakehouse/") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
        # .enableHiveSupport() \
        # .config("spark.packages", "org.postgresql:postgresql:42.2.1") \
        # .config("spark.sql.hive.metastore.jars", "/usr/local/airflow/jars/postgresql-42.2.1.jar") \
        # .master("spark://172.18.0.2:7077") \
        # .config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:postgresql://172.18.0.8:5432/metastore") \
        # .config("spark.sql.hive.hiveserver2.jdbc.user", "dbt") \
        # .config("spark.sql.hive.hiveserver2.jdbc.password", "dbt") \
        # .config("spark.packages", "org.postgresql:postgresql:42.3.7") \
        # .config("spark.sql.hive.metastore.jars", "/usr/local/airflow/jars/postgresql-42.3.7.jar") \
        # .config("spark.sql.hive.metastore.version", "2.3.7") \


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", aws_access_key)
hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("hive.metastore.uris", "thrift://172.18.0.7:10000")


def ingest():
    # Read the data
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    df = spark.read.json("s3a://raw-data/restaurants/yelp_academic_dataset_business.json")
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.restaurants")

def regis_table_use_pyhive():
    from pyhive import hive

    # Establish a connection to the Hive Metastore
    conn = hive.Connection(
        host='spark3-thrift',
        port=10000,
        username='dbt',
    )

    # Create a cursor object
    cursor = conn.cursor()

    # Create a new database
    cursor.execute("CREATE DATABASE IF NOT EXISTS bronze")

    # Set the database as the active database
    cursor.execute("USE bronze")

    # Create a Delta table with a specific path
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS restaurants
        USING delta
        LOCATION 's3a://prod/lakehouse/bronze.db/restaurants'
    """)

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()

ingest()
regis_table_use_pyhive()