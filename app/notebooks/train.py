from pyspark.sql import Window
from pyspark.sql.window import *
from pyspark.sql.functions import col, explode, row_number
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.recommendation import ALSModel
from datetime import datetime
######
### Main def of file
## rank, iter, reg is param of model
## justTrain: "model"(yes) or "rmse"(no) 
## fullData: "full data for train"(yes) or "split"(no)
def trainModel(data,rank=4,iters=4,reg=0.35,fullData=True,justTrain=True):
    newData=preprocessingData(data)
    if fullData:
            testData=newData
            model=train(newData,rank,iters,reg)
    else:
        trainData,testData=splitData(newData)
        model=train(trainData,rank,iters,reg)
    if justTrain==False:
        predictions=predictData(model,testData)
        rmse=evaluateResult(predictions)
        return rmse
    return model
######

def splitData(data,ratio=0.3):
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
    del newRowTest,newRowTrain
    return training,testing

def train(trainData,rank,iters,reg):
    als= ALS(rank=rank, maxIter=iters, regParam=reg, userCol="businessid", itemCol="userid", ratingCol="stars", nonnegative=True,implicitPrefs = False,coldStartStrategy="drop")
    model = als.fit(trainData)
    return model

def predictData(model,testData):
    predictions=model.transform(testData)
    return predictions

def evaluateResult(predictions):
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="stars", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    del evaluator
    return rmse

def saveModel(model,path):
    model.save(path)

def loadModel(path):
    model=ALSModel.load(path)
    return model

#numRe: num business for recommend
def recommendation(model,dfUser,numRe=5):
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