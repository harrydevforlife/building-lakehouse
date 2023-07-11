import os
import logging

import mlflow
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
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
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


def trainModel(data, rank=4, iters=4, reg=0.35, fullData=True, justTrain=True) -> ALSModel:
    """
    Attributes
    ----------
        rank, iter, reg is param of model
        fullData: "full data for train"(yes) or "split"(no)
        justTrain: "model"(yes) or "rmse"(no) 

    Methods
    -------
        train: training ALS model

    """
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("recommend_model")
    experiment = mlflow.get_experiment_by_name("recommend_model")

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name='spark_als_model'):
        print("Starting ...")

        newData = preprocessingData(data)

        if fullData:
            testData=newData
            trainData=newData
            model = train(trainData, rank, iters, reg)
        
            mlflow.log_metric("training_nrows", trainData.count())
            mlflow.log_metric("test_nrows", testData.count())
            print("Training: {}, test: {}".format(trainData.count(), testData.count()))
        else:
            trainData, testData = splitData(newData)
            model = train(trainData, rank, iters, reg)

            mlflow.log_metric("training_nrows", trainData.count())
            mlflow.log_metric("test_nrows", testData.count())
            print("Training: {}, test: {}".format(trainData.count(), testData.count()))
        if justTrain == False:
            predictionsTestData = predictData(model, testData)
            predictionsTrainData = predictData(model, trainData)

            test_mse =  evaluateResult(predictionsTestData)
            train_mse = evaluateResult(predictionsTrainData)

            print("The model had a MSE on the test set of {}".format(test_mse))
            print("The model had a MSE on the (train) set of {}".format(train_mse))
            mlflow.log_metric("test_mse", test_mse)
            mlflow.log_metric("train_mse", train_mse)

            # return test_mse, train_mse
        mlflow.spark.log_model(model, "als-model")
    return model


def train(trainData, rank, iters, reg) -> ALSModel:
    print("Training model")
    als= ALS(
        rank=rank, 
        maxIter=iters, 
        regParam=reg, 
        userCol="userid", 
        itemCol="businessid", 
        ratingCol="stars", 
        nonnegative=True, 
        implicitPrefs = False, 
        coldStartStrategy="drop"
    )
    model = als.fit(trainData)
    return model

def predictData(model, testData) -> ALSModel.transform:
    predictions=model.transform(testData)
    return predictions

def evaluateResult(predictions) -> RegressionEvaluator.evaluate:
    print("Evaluating on model...")
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="stars", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    return rmse

def saveModel(model, path):
    model.save(path)

def loadModel(path) -> ALSModel:
    model=ALSModel.load(path)
    return model

#numRe: num business for recommend
def recommendation(model, dfUser, numRe=5) -> ALSModel.recommendForUserSubset:
    userSubsetRecs=model.recommendForUserSubset(dfUser, numRe)
    userSubsetRecs = userSubsetRecs\
    .withColumn("rec_exp", explode("recommendations"))\
    .select('userid', col("rec_exp.businessid"), col("rec_exp.rating"))
    return userSubsetRecs

def preprocessingData(data):
    # To create userid (int of user_id with type string)
    userRatings=data.groupBy("user_id").count()
    ## take id type int
    window = Window.orderBy(col('user_id'))
    userRatings = userRatings.withColumn('userid', row_number().over(window))
    userRatings= userRatings.select('user_id','userid')

    # As same as with business
    buRatings=data.groupBy("business_id").count()
    window = Window.orderBy(col('business_id'))
    buRatings = buRatings.withColumn('businessid', row_number().over(window))
    buRatings= buRatings.select('business_id','businessid')

    # Join to get cols userid and businessid
    newratings=data.join(userRatings, ['user_id'])
    newratings=newratings.join(buRatings, ['business_id'])
    del userRatings
    del buRatings
    return newratings

def splitData(data, ratio=0.3):
    print("Splitting data to training and testing data ...")
    ### First split Data
    # get num row of test Data
    num=int(data.count()*ratio)
    # get test data by latest date
    ortherDate=data.orderBy(col("date").desc())
    testing=ortherDate.limit(int(num))
    # get train data
    training=ortherDate.subtract(testing)
    del ortherDate

    ### Then confuse data
    # new is test data that's no userid in train data
    userTrain=training.groupBy('userid').count().select('userid')
    duplicate=testing.join(userTrain,['userid'])
    new=testing.subtract(duplicate)
    del userTrain, duplicate
    # newRowTrain: get 0.3 of new data to join to train data
    newRowTrain,_=new.randomSplit([0.3,0.7]) 
    del new
    # num row of new row train
    numNewTrain=newRowTrain.count()
    # newRowTest: and collect with same row num by latest date in train data to join to test data
    newRowTest=training.orderBy(col("date").desc()).limit(numNewTrain)
    del numNewTrain
    # Each test/train data, remove old and add new data
    training=training.subtract(newRowTest).union(newRowTrain)
    testing=testing.subtract(newRowTrain).union(newRowTest)
    training = (training
        .withColumn("userid", col("userid").cast("int"))
        .withColumn("businessid", col("businessid").cast("int"))
        .withColumn("stars", col("stars").cast("int"))
    )
    testing = (testing
        .withColumn("userid", col("userid").cast("int"))
        .withColumn("businessid", col("businessid").cast("int"))
        .withColumn("stars", col("stars").cast("int"))
    )
    del newRowTest,newRowTrain
    return training,testing


if __name__ == "__main__":
    review_dataset = spark.table("feature_store.review")
    trainModel(review_dataset, rank=4, iters=4, reg=0.35, fullData=False, justTrain=False)