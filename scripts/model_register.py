import os
import logging
import time

import mlflow
from mlflow.tracking import MlflowClient
import pyspark
from pyspark.sql import Window
from pyspark.sql.window import *
from pyspark.sql.functions import col, explode, row_number
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel

AWS_ACCESS_KEY = 'minioaws'
AWS_SECRET_KEY = 'minioaws'
AWS_S3_ENDPOINT = 'http://minio:9000'
AWS_BUCKET_NAME = 'lakehouse'

os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["AWS_BUCKET"] = "mlflow"
os.environ["FILE_DIR"] = "/mlflow"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

# def spark_session(spark_session):

#     spark = spark_session.builder \
#         .appName('Training ALS model') \
#         .master('spark://spark-master:7077') \
#         .config("hive.metastore.uris", "thrift://hive-metastore:9083")\
#         .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
#         .config("spark.hadoop.fs.s3a.secret.key", AWS_ACCESS_KEY) \
#         .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)\
#         .config("spark.hadoop.fs.s3a.path.style.access", "true")\
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
#         .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
#         .config('spark.sql.warehouse.dir', f's3a://{AWS_BUCKET_NAME}/')\
#         .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')\
#         .config('spark.driver.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
#         .config('spark.executor.extraClassPath', '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/s3-2.18.41.jar/opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar/opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar/opt/bitnami/spark/jars/delta-storage-2.2.0.jar')\
#         .enableHiveSupport()\
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("ERROR")

#     return spark

# spark = spark_session(pyspark.sql.SparkSession)

def find_best_run_id(run_name: str) -> str:
    best_run = mlflow.search_runs(order_by=['metrics.test_mse DESC']).iloc[0]
    print(f'MSE of Best Run: {best_run["metrics.test_mse"]}')
    return best_run


def find_run_id(run_name: str) -> str:
    run_id = mlflow.search_runs(filter_string=f'tags.mlflow.runName = "{run_name}"').iloc[0].run_id
    return run_id

def register_model(model_name: str = "restaurant_recommender"):
    model_version = mlflow.register_model(f"runs:/{run_id}/als-model", model_name)
    time.sleep(15)
    return model_version

def transit_model(model_name: str, model_version: str, stage: str):
    client = MlflowClient()
    client.transition_model_version_stage(
    name=model_name,
    version=model_version,
    stage=stage,
    )

if __name__ == "__main__":
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("recommend_model")
    run_id = find_run_id(run_name="spark_als_model")
    model_version = register_model()
    transit_model(
            model_name="restaurant_recommender",
            model_version=model_version.version,
            stage="Production"
        )
    if model_version.version != 1:
        old_model_version = int(model_version.version) - 1
        transit_model(
            model_name="restaurant_recommender",
            model_version=old_model_version,
            stage="Archived"
        )