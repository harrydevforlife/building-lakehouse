import os
from pyspark.sql import SparkSession
from datetime import date

from sparksession import spark_session


spark = spark_session(SparkSession)

df = spark.read.json("s3a://raw-data/yelp/json/yelp_academic_dataset_review.json")
df.write.format("delta").mode("overwrite").saveAsTable("bronze.review")
spark.sql("SHOW TABLES IN bronze").show()
