import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

def hello_world():
    print("Hello World")
    print(os.environ.get("SPARK_HOME"))

with DAG(dag_id="etl", start_date=datetime(2020, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    start = DummyOperator(task_id="start")
    hello_world = PythonOperator(task_id="hello_world", python_callable=hello_world)
    end = DummyOperator(task_id="end")

    start >> hello_world >> end


