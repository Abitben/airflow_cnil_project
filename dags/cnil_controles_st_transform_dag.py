"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

import sys
import os
sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from include.dbt.open_cnil.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG, DBT_EXECUTION_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from airflow.sensors.external_task import ExternalTaskSensor

# adjust for other database types
from pendulum import datetime
import os
from pathlib import Path

# The path to the dbt project
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

@dag(
    start_date=datetime(2023, 8, 1),
    schedule='@weekly',
    catchup=False,
)
def dbt_controles_st_dag():

    # sensors_sourcing_task = ExternalTaskSensor(
    #     task_id='trigger_sourcing',
    #     external_dag_id='sourcing_dag',
    #     # external_task_id='upload_to_bq',
    # )

    controles_stats_task = DbtTaskGroup(
        group_id="controles_stats",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        render_config=RenderConfig(
              load_method=LoadMode.DBT_LS,
              select=['+fct_controles_stats'],
          )
    )


    controles_stats_task
    
dbt_controles_st_dag()