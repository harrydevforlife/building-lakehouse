import os
from datetime import timedelta
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def set_enviroment():
    print("SET ENV")
    os.environ["SPARK_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HADOOP_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HIVE_HOME"] = "/usr/local/airflow/jars"
    os.environ["AWS_ACCESS_KEY_ID"] = "minio"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minio123"
    os.environ["AWS_BUCKET"] = "mlflow"
    os.environ["FILE_DIR"] = "/mlflow"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"


with DAG('mlflow_model', default_args=default_args,  schedule_interval=None) as dag:
    set_env = PythonOperator(task_id="set_enviroment", python_callable=set_enviroment)
    feature_store = SparkSubmitOperator(
        task_id='feature_store',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='2',
        driver_memory='1g',
        application="../airflow/scripts/feature_store.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

    train_model = SparkSubmitOperator(
        task_id='train_model',
        total_executor_cores='4',
        executor_cores='2',
        executor_memory='4g',
        num_executors='2',
        driver_memory='2g',
        application="../airflow/scripts/train_model.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

    register_model = BashOperator(
        task_id="register_model", 
        bash_command="python3 $AIRFLOW_HOME/scripts/model_register.py"
        )


    set_env >> feature_store >> train_model >> register_model