import os
from pyspark.sql import SparkSession
from datetime import date

from sparksession import spark_session


spark = spark_session(SparkSession)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS feature_store")
spark.sql("SHOW DATABASES").show()
