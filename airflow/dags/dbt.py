import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def hello_world():
    print("Hello World")
    print(os.environ.get("SPARK_HOME"))

with DAG(dag_id="run_dbt", start_date=datetime(2020, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    run_dbt = BashOperator( 
        task_id="run_dbt",
        bash_command="dbt run",
        env={"SPARK_HOME": "/usr/local/airflow/jars"}
    )

    run_dbt 

