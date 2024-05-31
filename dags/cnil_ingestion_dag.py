import sys
import os
sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.python import ExternalPythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom import XCom
from include.sourcing.catalogs_cnil_tasks import compare_and_upload_catalogs
from include.sourcing.download_cnil_content import download_cnil_content
from include.sourcing.prepare_cnil_data import prepare_cnil_data
from include.sourcing.upload_cnil_gbq import upload_cnil_gbq
from include.sourcing.update_catalog_bq import update_catalog_bq

# adjust for other database types
from pendulum import datetime
import os
from pathlib import Path


@dag(
    start_date=datetime(2023, 8, 1),
    schedule='@weekly',
    catchup=False,
)
def sourcing_dag():

    upload_catalogs = ExternalPythonOperator(
        task_id='upload_compare_catalogs',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=compare_and_upload_catalogs,
    )

    download_content = ExternalPythonOperator(
        task_id='download_cnil_content',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=download_cnil_content,
    )

    prepare_data = ExternalPythonOperator(
        task_id='prepare_cnil_data',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=prepare_cnil_data,
    )

    upload_gbq = ExternalPythonOperator(
        task_id='upload_cnil_gbq',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=upload_cnil_gbq,
    )

    upload_catalogs_updated = ExternalPythonOperator(
        task_id='upload_catalogs_updated',
        python=os.environ["ASTRO_PYENV_custom_python"],
        python_callable=update_catalog_bq,
    )

    upload_catalogs >> download_content >> prepare_data >> upload_gbq >> upload_catalogs_updated

sourcing_dag()