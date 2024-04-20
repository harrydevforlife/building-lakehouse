import os

from pyspark.sql import Window
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, regexp_replace, lower
from pyspark.sql import SparkSession

from sparksession import spark_session


spark = spark_session(SparkSession)

def preprocessing(data):
    data = data.select("user_id", "name", "friends")
    user_id_data=data.groupBy("user_id").count()
    ## take id type int
    window = Window.orderBy(col('user_id'))
    user_id_data = user_id_data.withColumn('userid', row_number().over(window))
    user_id_data= user_id_data.select('user_id','userid')

    new_data=data.join(user_id_data, ['user_id'])

    return new_data

df = spark.table("bronze.user")
df = preprocessing(df)
df =(df
    .withColumn("account_name", regexp_replace("name", " ", ""))
    .withColumn("account_name", lower("account_name"))
    .withColumn("account_pass", col("account_name"))
)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.user_account")
