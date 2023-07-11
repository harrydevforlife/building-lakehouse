import json, os
import numpy as np
import mlflow
from flask import Flask, request, redirect, url_for, flash, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


app = Flask(__name__)


spark = SparkSession.builder.appName('Recommendation system').getOrCreate()


os.environ["AWS_ACCESS_KEY_ID"] = "minio"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
os.environ["AWS_BUCKET"] = "mlflow"
os.environ["FILE_DIR"] = "/mlflow"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("recommend_model")

@app.route('/api/', methods=['POST'])
def makecalc():
    data = request.get_json()
    user_id = data["userid"]
    res_num = data["res_num"]
    token = data["token"]
    if data["token"] != "systemapi":
        return f"Token not validate !"
        
    print(user_id, res_num) 
    json_path = write_file(data)
    user_data = prepare_data(json_path) 
    model = load_model() 
    prediction = predict(model, user_data, res_num)
    prediction.show() 
    return df_to_json(prediction) 

def write_file(data):
    with open("/opt/mlflow/data.json", "w") as f:
        json.dump(data, f)
    return "/opt/mlflow/data.json"

def load_model():
    print("Loading model...")
    model = mlflow.spark.load_model("models:/restaurant_recommender/Production")
    return model

def prepare_data(data):
    print("Create dataframe...")
    df = spark.read.json(data).select("userid")
    df.show() 
    return df

def df_to_json(data):
    recommend_df = (data
        .withColumn("rec_exp", explode("recommendations"))\
        .select('userid', col("rec_exp.businessid"), col("rec_exp.rating"))
    )
    list_of_recommend = recommend_df.collect()

    alls = {
        "userid": "",
        "results": [],
        "businessid": [],
        "rating": []
    }
    for row in list_of_recommend:
        alls["userid"] = row[0]
        alls["businessid"].append(row[1])
        alls["rating"].append(row[2])
        alls["results"].append(
            {
                "businessid": row[1],
                "rating": row[2]
            }
        )

    return json.dumps(alls)
 
def predict(model, data, num):
    print("Predicting...")
    return model.stages[0].recommendForUserSubset(data, num)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port='5001')