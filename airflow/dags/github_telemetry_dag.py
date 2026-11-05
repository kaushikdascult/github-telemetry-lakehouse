"""
Airflow DAG: GitHub Telemetry Lakehouse Orchestrator.
Schedule: Every 15 minutes (continuous micro-batch) or @hourly.
Flow: extract_to_bq >> dbt_run >> dbt_test
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Environment detection: auto-resolve container mounts vs host workspace
IN_CONTAINER = os.path.exists("/opt/airflow/ingestion")

if IN_CONTAINER:
    PROJECT_DIR = "/opt/airflow"
    VENV_PYTHON = os.getenv("VENV_PYTHON", "python3")
    VENV_DBT = os.getenv("VENV_DBT", "dbt")
else:
    PROJECT_DIR = os.getenv("PROJECT_DIR", "/home/datadask/MyProject/github-telemetry-lakehouse")
    VENV_PYTHON = os.getenv("VENV_PYTHON", "/home/datadask/my_python312_env/bin/python")
    VENV_DBT = os.getenv("VENV_DBT", "/home/datadask/my_python312_env/bin/dbt")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="github_telemetry_lakehouse",
    default_args=default_args,
    description="Continuous micro-batch telemetry pipeline: GitHub API → BigQuery Sandbox → dbt",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["github", "telemetry", "bigquery", "dbt", "lakehouse"],
) as dag:

    # Task 1: Fetch from GitHub REST API and stream to BigQuery Bronze
    extract_to_bq = BashOperator(
        task_id="extract_api_to_bq_bronze",
        bash_command=f"{VENV_PYTHON} {PROJECT_DIR}/ingestion/fetch_and_push_bq.py",
    )

    # Task 2: Execute dbt Silver & Gold dimensional transformations
    run_dbt = BashOperator(
        task_id="dbt_run_transformations",
        bash_command=f"cd {PROJECT_DIR}/dbt && {VENV_DBT} run --profiles-dir .",
    )

    # Task 3: Enforce data quality rules (uniqueness, not null, valid timestamps)
    test_dbt = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"cd {PROJECT_DIR}/dbt && {VENV_DBT} test --profiles-dir .",
    )

    extract_to_bq >> run_dbt >> test_dbt
