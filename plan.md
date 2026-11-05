# Codex Architecture & Engineering Review: GitHub Telemetry Lakehouse

## What It Does

This is a lightweight ELT pipeline: it fetches recent public GitHub events, appends them to a BigQuery raw table, uses dbt to deduplicate and reshape them, and builds an analytics table with regional timestamps and activity-hour flags. Orchestration can be executed manually with `just`, on a schedule via GitHub Actions, or through Apache Airflow.

* **Ingestion Worker**: [ingestion/fetch_and_push_bq.py](file:///home/datadask/MyProject/github-telemetry-lakehouse/ingestion/fetch_and_push_bq.py)
* **dbt Transformations**: [dbt/models](file:///home/datadask/MyProject/github-telemetry-lakehouse/dbt/models)
* **GitHub Actions Workflow**: [.github/workflows/pipeline.yml](file:///home/datadask/MyProject/github-telemetry-lakehouse/.github/workflows/pipeline.yml)
* **Airflow Orchestrator DAG**: [airflow/dags/github_telemetry_dag.py](file:///home/datadask/MyProject/github-telemetry-lakehouse/airflow/dags/github_telemetry_dag.py)
* **Airflow Container Stack**: [airflow/docker-compose.yml](file:///home/datadask/MyProject/github-telemetry-lakehouse/airflow/docker-compose.yml)
* **Gold Mart Model**: [dbt/models/marts/fct_global_developer_activity.sql](file:///home/datadask/MyProject/github-telemetry-lakehouse/dbt/models/marts/fct_global_developer_activity.sql)

---

## Main Improvements to Make

### 1. Re-evaluate the "Burnout" Interpretation [STATUS: COMPLETED]
* **Resolution**: Replaced subjective `is_midnight_oil_pushed` with objective `is_off_peak_activity` across [fct_global_developer_activity.sql](file:///home/datadask/MyProject/github-telemetry-lakehouse/dbt/models/marts/fct_global_developer_activity.sql), [dbt/models/marts/_schema.yml](file:///home/datadask/MyProject/github-telemetry-lakehouse/dbt/models/marts/_schema.yml), [justfile](file:///home/datadask/MyProject/github-telemetry-lakehouse/justfile), and [README.md](file:///home/datadask/MyProject/github-telemetry-lakehouse/README.md).
* **Documentation**: Clarified that late-night activity flags represent temporal offset estimates rather than actor domicile or physical working hours.

### 2. Event Coverage, Sampling & Ingestion Semantics [STATUS: PENDING]
The GitHub Public Events API (`/events`) returns a rolling window of recent events (~300 events or 30-100 per page). Polling periodically without event checkpointing does not guarantee an uninterrupted stream. Furthermore, overlapping polling windows append duplicate rows into the raw layer.
* **Actionable Change**: Implement pagination, cursor checkpointing (e.g., tracking `ETag` and `Last-Modified` headers, or caching maximum seen `event_id`), and log gap indicators.
* **Documentation**: Accurately document Bronze data as sampled public activity rather than full exhaustive developer telemetry.

### 3. Make Airflow Container Setup Runnable [STATUS: COMPLETED]
* **Resolution**: Updated [airflow/dags/github_telemetry_dag.py](file:///home/datadask/MyProject/github-telemetry-lakehouse/airflow/dags/github_telemetry_dag.py) to dynamically detect container runtime mounts (`/opt/airflow/ingestion`) versus local host development environments (`/home/datadask/...`), using containerized `python3` and `dbt` executables when containerized.
* **Security**: Updated [airflow/docker-compose.yml](file:///home/datadask/MyProject/github-telemetry-lakehouse/airflow/docker-compose.yml) to utilize configurable environment credentials (`AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`) instead of static hardcoded passwords.

### 4. Optimize BigQuery Compute & Sandbox Quotas [STATUS: RESOLVED VIA BOUNDED DDL CTAS]
* **Constraint Discovery & Resolution**: Tested dbt `materialized='incremental'` with `merge` strategy against BigQuery Sandbox. BigQuery returned: `Billing has not been enabled for this project. DML queries are not allowed in the free tier.` BigQuery Sandbox strictly prohibits all DML (`MERGE`, `UPDATE`, `DELETE`) without an active billing account.
* **Architectural Fix**: Maintained atomic DDL table replacement (`materialized='table'`), which BigQuery Sandbox permits for free. Hard-bounded the input scan in the model to `created_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)`, perfectly aligned with the Bronze layer's automatic 7-day partition TTL. This ensures query bytes scanned remain capped at ~192 KiB per execution without risking monthly Sandbox quota depletion.

### 5. Centralize Configuration Across Components
Project and dataset IDs (`kd-bq-sandbox-2026`, `github_bronze`, `github_silver`, `github_marts`) are scattered across [scripts/setup_bigquery.py](file:///home/datadask/MyProject/github-telemetry-lakehouse/scripts/setup_bigquery.py), [dbt/dbt_project.yml](file:///home/datadask/MyProject/github-telemetry-lakehouse/dbt/dbt_project.yml), [justfile](file:///home/datadask/MyProject/github-telemetry-lakehouse/justfile), and workflow YAMLs.
* **Actionable Change**: Standardize configuration across ingestion, dbt profiles, scripts, and orchestration through centralized environment variables and unified defaults to prevent cross-environment drift.

### 6. Rationalize SLA, Cost, and Reliability Claims
Google BigQuery Sandbox imposes rigid operational limits (table partition expiration, concurrency limits, 1 TB monthly query processing ceiling).
* **Actionable Change**: Frame the architecture as an educational zero-cost sandbox reference pipeline. Clearly articulate Sandbox constraints, failure modes, and trade-offs in [README.md](file:///home/datadask/MyProject/github-telemetry-lakehouse/README.md) and architectural decision records (ADRs).

---

## Credentials & Authentication Architecture

* **Local Ingestion & dbt**: Uses Google Application Default Credentials (ADC) via `gcloud auth application-default login`. Requires `roles/bigquery.jobUser` and `roles/bigquery.dataEditor`.
* **GitHub Public API**: Public events do not strictly require an API token, but injecting `GITHUB_TOKEN` raises the rate limit from 60 to 5,000 requests/hour.
* **GitHub Actions Workflow**: Currently relies on a long-lived service account key stored in `GCP_SA_KEY`. Target modern enterprise security best practices by migrating to **Workload Identity Federation (OIDC)** to eliminate static keys.
* **Airflow Container Execution**: Requires the mounted host ADC volume (`~/.config/gcloud`) to have valid permissions and point `GOOGLE_APPLICATION_CREDENTIALS` to the container path `/opt/airflow/.config/gcloud/application_default_credentials.json`.
