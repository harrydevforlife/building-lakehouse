import json
from datetime import datetime
from airflow import DAG
from dbt_task_generator import DbtTaskGenerator

with open("/usr/local/airflow/restaurant_analytis/target/manifest.json") as f:
    manifest = json.load(f)

dag = DAG(
    dag_id="dbt_connected_task_creator_test_dag",
    start_date=datetime(2019, 1, 1),
    schedule_interval="0 1 * * *",
)
dbt_task_generator = DbtTaskGenerator(dag, manifest)
dbt_task_generator.add_all_tasks()