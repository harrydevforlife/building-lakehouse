"""
Trains an Alternating Least Squares (ALS) model for user/movie ratings.
The input is a Parquet ratings dataset (see etl_data.py), and we output
an mlflow artifact called 'als-model'.
"""
from random import random, randint
import os
import click  

import mlflow
import pyspark
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

AWS_ACCESS_KEY = 'minioaws'
AWS_SECRET_KEY = 'minioaws'
AWS_S3_ENDPOINT = 'http://minio:9000'
AWS_BUCKET_NAME = 'lakehouse'

os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["AWS_BUCKET"] = "mlflow"
os.environ["FILE_DIR"] = "/mlflow"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

def spark_session(spark_session):

    spark = spark_session.builder \
        .appName('Training ALS model') \
        .master('spark://spark-master:7077') \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")\
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config('spark.sql.warehouse.dir', f's3a://{AWS_BUCKET_NAME}/')\
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')\
        .config('spark.driver.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
        .config('spark.executor.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
        .enableHiveSupport()\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark

spark = spark_session(pyspark.sql.SparkSession)


@click.command()
@click.option("--table-data", default="feature_store.review", type=str)
@click.option("--split-prop", default=0.8, type=float)
@click.option("--max-iter", default=10, type=int)
@click.option("--reg-param", default=0.1, type=float)
@click.option("--rank", default=12, type=int)
@click.option("--cold-start-strategy", default="drop")
def train_als(table_data, split_prop, max_iter, reg_param, rank, cold_start_strategy):
    seed = 42
    print("Running the test script ...")

    mlflow.set_tracking_uri("http://mlflow:5000")

    ratings_df = spark.table(table_data)
    
    (training_df, test_df) = ratings_df.randomSplit([split_prop, 1 - split_prop], seed=seed)
    training_df.cache()
    test_df.cache()

    mlflow.log_metric("training_nrows", training_df.count())
    mlflow.log_metric("test_nrows", test_df.count())

    print("Training: {}, test: {}".format(training_df.count(), test_df.count()))

    als = (
        ALS()
        .setUserCol("user_id")
        .setItemCol("business_id")
        .setRatingCol("stars")
        .setPredictionCol("predictions")
        .setMaxIter(max_iter)
        .setSeed(seed)
        .setRegParam(reg_param)
        .setColdStartStrategy(cold_start_strategy)
        .setRank(rank)
    )

    als_model = Pipeline(stages=[als]).fit(training_df)

    reg_eval = RegressionEvaluator(predictionCol="predictions", labelCol="rating", metricName="mse")

    predicted_test_dF = als_model.transform(test_df)

    test_mse = reg_eval.evaluate(predicted_test_dF)
    train_mse = reg_eval.evaluate(als_model.transform(training_df))

    print("The model had a MSE on the test set of {}".format(test_mse))
    print("The model had a MSE on the (train) set of {}".format(train_mse))
    mlflow.log_metric("test_mse", test_mse)
    mlflow.log_metric("train_mse", train_mse)
    mlflow.spark.log_model(als_model, "als-model")


    # #Connect to tracking server
    # mlflow.set_tracking_uri("http://mlflow:5000")

    # #Create directory for artifacts
    # if not os.path.exists("artifact_folder"):
    #     os.makedirs("artifact_folder")

    # all_tables = spark.sql("show tables in bronze")
    # print(all_tables.show())

    # mlflow.log_param("table_data", all_tables.show())

    # #Test parametes
    # mlflow.log_param("param1", randint(0, 100))

    # #Test metrics
    # mlflow.log_metric("metric1", random())
    # mlflow.log_metric("metric1", random())
    # mlflow.log_metric("metric1", random())
    # mlflow.log_metric("metric1", random())

    # #Test artifact
    # with open("artifact_folder/test.txt", "w") as f:
    #     f.write("hello world! - als")
    # mlflow.log_artifacts("artifact_folder")


if __name__ == "__main__":
    train_als()