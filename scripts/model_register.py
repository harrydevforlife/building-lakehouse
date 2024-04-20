import time

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql.window import *


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