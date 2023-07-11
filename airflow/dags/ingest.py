import os
from datetime import timedelta
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME, "restaurant_analytis") # path to your dbt project
manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'schedule_interval': '15 08 * * *',
    'retry_delay': timedelta(seconds=5),
    'dagrun_timeout': timedelta(hours=12),
}

def set_enviroment():
    print("SET ENV")
    os.environ["SPARK_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HADOOP_CONF_DIR"] = "/usr/local/airflow/jars/conf"
    os.environ["HIVE_HOME"] = "/usr/local/airflow/jars"
    print(os.environ.get("SPARK_CONF_DIR"))
    print(os.environ.get("HADOOP_CONF_DIR"))
    print(os.environ.get("HIVE_HOME"))


with DAG('etl_pipeline', default_args=default_args,  schedule_interval=None) as dag:
    set_env = PythonOperator(task_id="set_enviroment", python_callable=set_enviroment)
    start_transform = DummyOperator(task_id="start_transform")
    create_schema = SparkSubmitOperator(
        task_id='create_schema',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/create_schema.py",
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
        application="../airflow/scripts/bronze_tip.py",
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
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_user.py",
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
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_restaurant.py",
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
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_review.py",
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
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_checkin.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    transform_user_account = SparkSubmitOperator(
        task_id='transform_user_account',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_user_account.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )
    transform_restaurant = SparkSubmitOperator(
        task_id='transform_restaurant',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='1',
        driver_memory='1g',
        application="../airflow/scripts/bronze_business_transform.py",
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        jars="../airflow/jars/hadoop-aws-3.3.4.jar,../airflow/jars/s3-2.18.41.jar,../airflow/jars/aws-java-sdk-1.12.367.jar,../airflow/jars/delta-core_2.12-2.2.0.jar,../airflow/jars/delta-storage-2.2.0.jar,../airflow/jars/mysql-connector-java-8.0.19.jar",
        conn_id="spark_master" , # Connection ID for your Spark cluster configuration
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    )

        # Create a dict of Operators
    dbt_tasks = dict()
    for node_id, node_info in nodes.items():
        if node_info["resource_type"] == "model":
            dbt_tasks[node_id] = BashOperator(
                task_id=".".join(
                    [
                        node_info["package_name"],
                        node_info["name"],
                    ]
                ),
                bash_command=f"cd {dbt_path}" # Go to the path containing your dbt project
                + f" && dbt run --models {node_info['name']}", # run the model!
            )

    # Define relationships between Operators
    for node_id, node_info in nodes.items():
        if node_info["resource_type"] == "model":
            upstream_nodes = node_info["depends_on"]["nodes"]
            if upstream_nodes:
                    for upstream_node in upstream_nodes:
                        if upstream_node in dbt_tasks:
                            dbt_tasks[upstream_node] >> dbt_tasks[node_id]

set_env >> create_schema >> ingest_review >> ingest_tip >> ingest_user >> ingest_restaurant >> ingest_checkin >> [transform_user_account, transform_restaurant] >> start_transform >> list(dbt_tasks.values())
# # start >> set_env >> ingest_checkin >> end
# set_env >> create_schema
# create_schema >> [ingest_tip, ingest_user, ingest_restaurant, ingest_review, ingest_checkin] >> start_transform
# start_transform >> list(dbt_tasks.values())
