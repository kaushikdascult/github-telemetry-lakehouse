# GitHub Telemetry Lakehouse task runner
# Uses the pre-installed my_python312_env with zero extra disk footprint

PYTHON := "/home/datadask/my_python312_env/bin/python"
DBT := "/home/datadask/my_python312_env/bin/dbt"

# Default recipe: show help
default:
    @just --list

# Provision BigQuery Sandbox datasets and partitioned bronze table
setup-bq:
    {{ PYTHON }} scripts/setup_bigquery.py

# Ingest single micro-batch from GitHub Events API directly to BigQuery Bronze
ingest:
    {{ PYTHON }} ingestion/fetch_and_push_bq.py

# Execute dbt transformations (Silver staging & Gold marts)
dbt-run:
    cd dbt && {{ DBT }} run --profiles-dir .

# Run dbt generic and singular data quality tests
dbt-test:
    cd dbt && {{ DBT }} test --profiles-dir .

# Execute full end-to-end batch (Ingest -> dbt Run -> dbt Test)
run:
    {{ PYTHON }} scripts/run_pipeline.py

# Inspect top 10 recent records in BigQuery Gold Mart
query:
    {{ PYTHON }} -c "from google.cloud import bigquery; client = bigquery.Client(project='kd-bq-sandbox-2026'); print(client.query('SELECT event_id, event_type, actor_login, amer_local_time, is_off_peak_activity FROM kd-bq-sandbox-2026.github_marts.fct_global_developer_activity ORDER BY created_at_utc DESC LIMIT 10').to_dataframe().to_string())"
