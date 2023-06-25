import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
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
    create_schema = SparkSubmitOperator(
        task_id='create_schema',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/create_schema.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    ingest_tip = SparkSubmitOperator(
        task_id='ingest_tip',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_tip.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    ingest_user = SparkSubmitOperator(
        task_id='ingest_user',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_user.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    ingest_restaurant = SparkSubmitOperator(
        task_id='ingest_restaurant',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_restaurant.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    ingest_review = SparkSubmitOperator(
        task_id='ingest_review',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_review.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    ingest_checkin = SparkSubmitOperator(
        task_id='ingest_checkin',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='1g',
        application="../scripts/bronze_checkin.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
start >> set_env >> create_schema >> ingest_tip >> ingest_user >> ingest_restaurant >> ingest_review >> ingest_checkin >> end
# start >> set_env >> ingest_checkin >> end
