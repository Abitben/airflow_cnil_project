from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
import os
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='open_cnil',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/open_cnil/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/open_cnil',
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path= f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
)