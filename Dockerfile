# syntax=quay.io/astronomer/airflow-extensions:v1

FROM quay.io/astronomer/astro-runtime:11.4.0

USER root

RUN apt-get update && apt-get install -y \
    libxml2-dev \
    libxslt-dev \
    gcc

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

PYENV 3.10 custom_python custom_python-requirements.txt