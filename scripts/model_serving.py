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

def serving_model():
    os.system(" export MLFLOW_TRACKING_URI=http://mlflow:5000 && \
    export AWS_ACCESS_KEY_ID=minio && \
    export AWS_SECRET_ACCESS_KEY=minio123 && \
    export AWS_BUCKET=mlflow && \
    export FILE_DIR=/mlflow && \
    export MLFLOW_S3_ENDPOINT_URL=http://minio:9000 && \
    mlflow models serve -m 'models:/restaurant_recommender/Production'")

if __name__ == "__main__":
    serving_model()