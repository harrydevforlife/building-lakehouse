import os
from pyspark.sql import SparkSession
from datetime import date

from sparksession import spark_session


spark = spark_session(SparkSession)

spark.sql("CREATE DATABASE IF NOT EXISTS feature_store")
df = spark.read.table("bronze.review")
df = df.select("business_id", "user_id", "stars", "date")
df.write.format("delta").mode("overwrite").saveAsTable("feature_store.review")
spark.sql("SHOW TABLES IN feature_store").show()
