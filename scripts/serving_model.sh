#!/bin/bash

# cd $AIRFLOW_HOME
# curl https://pyenv.run | bash
# python -m  pip install virtualenv
# PATH="$AIRFLOW_HOME/.pyenv/bin:$PATH"

# Set environment variable for the tracking URL where the Model Registry resides
cd $AIRFLOW_HOME

# python3 -m venv .env
# source .env.bin/activate
# pip install mlflow==2.3
# pip install pa

export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
export AWS_BUCKET=mlflow
export FILE_DIR=/mlflow
export MLFLOW_S3_ENDPOINT_URL=http://minio:9000
export MLFLOW_TRACKING_URI=http://mlflow:5000 

# mlflow models serve -m "models:/restaurant_recommender/Production" --env-manager local --no-conda --timeout 180

mlflow models serve -m "s3://mlflow/artifacts/1/0610f6fe239f4bd89413970f6c83e0d7/artifacts/als-model" -p 5001 --env-manager local --no-conda --timeout 180