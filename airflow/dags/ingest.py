import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def set_enviroment():
    print("SET ENV")
    os.environ["SPARK_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HADOOP_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HIVE_HOME"] = "/usr/local/airflow/jars"
    print(os.environ.get("SPARK_CONF_DIR"))
    print(os.environ.get("HADOOP_CONF_DIR"))
    print(os.environ.get("HIVE_HOME"))


with DAG('etl_pipeline', default_args=default_args, schedule_interval=None) as dag:
    start = DummyOperator(task_id="start")
    set_env = PythonOperator(task_id="set_enviroment", python_callable=set_enviroment)
    end = DummyOperator(task_id="end")
    ingest_tip = SparkSubmitOperator(
        task_id='load_tip',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_tip.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        # files="/usr/local/airflow/jars/conf/hive-site.xml",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

start >> set_env >> ingest_tip >> end
