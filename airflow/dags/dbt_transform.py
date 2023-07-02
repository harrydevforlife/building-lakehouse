import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

HOME = os.environ["HOME"] # retrieve the location of your home folder
dbt_path = os.path.join(HOME, "restaurant_analytis") # path to your dbt project
manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

with open(manifest_path) as f: # Open manifest.json
  manifest = json.load(f) # Load its contents into a Python Dictionary
  nodes = manifest["nodes"] # Extract just the nodes

def set_env():
    print("Setting dbt environment variables")
    os.environ["DBT_PROFILES_DIR"] = "$AIRFLOW_HOME/restaurant_analytis"
    os.environ["DBT_PROJECT_DIR"] = "$AIRFLOW_HOME/restaurant_analytis"


# Build an Airflow DAG
with DAG(
  dag_id="dbt_transform", # The name that shows up in the UI
  start_date=datetime(2023, 1, 1),
  catchup=False,
) as dag:

  # Set environment variables
  set_environment = PythonOperator(task_id="set_environment", python_callable=set_env)

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

  set_environment >> list(dbt_tasks.values()) # Set environment variables before running dbt