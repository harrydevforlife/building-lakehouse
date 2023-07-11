import json, os
import numpy as np
import mlflow
from mlflow import spark
from flask import Flask, request, redirect, url_for, flash, jsonify


app = Flask(__name__)


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

    user_data = prepare_data(data)
    model = load_model()
    prediction = predict(model, user_data)
    
    return jsonify(prediction)

def load_model():
    model = mlflow.spark.load_model("models:/restaurant_recommender/Production")
    return model

def prepare_data(data):
    return spark.createDataFrame(data=data, schema = ['userid'])

def predict(model, data):
    return model.transform(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')