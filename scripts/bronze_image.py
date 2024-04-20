import os
from pyspark.sql import SparkSession
from datetime import date

from sparksession import spark_session


spark = spark_session(SparkSession)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze")
df = spark.read.format("image").option("dropInvalid", True).load("s3a://raw-data/yelp/photos")
df.select("image.origin", "image.width", "image.height").show(10)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.images")
spark.sql("SHOW DATABASES").show()
