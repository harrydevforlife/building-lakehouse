from pyspark.sql import Window
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql import SparkSession

from sparksession import spark_session


spark = spark_session(SparkSession)

def preprocessing(data):
    user_id_data=data.groupBy("business_id").count()
    ## take id type int
    window = Window.orderBy(col('business_id'))
    user_id_data = user_id_data.withColumn('businessid', row_number().over(window))
    user_id_data= user_id_data.select('business_id','businessid')

    new_data=data.join(user_id_data, ['business_id'])
    return new_data

df = spark.table("bronze.restaurant")
df = preprocessing(df)
df.write.format("delta").mode("overwrite").saveAsTable("bronze.restaurant_transform")
