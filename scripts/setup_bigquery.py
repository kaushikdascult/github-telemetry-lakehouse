"""
Bootstrap BigQuery Sandbox Datasets & Partitioned Tables for GitHub Telemetry Lakehouse.
Project: kd-bq-sandbox-2026
Datasets:
  - github_bronze (Raw JSON payloads, 7-day partition expiration)
  - github_silver (Staging dbt models)
  - github_marts  (Analytical Gold models)
"""

import sys
import logging
from google.cloud import bigquery
from google.cloud.exceptions import Conflict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("setup_bigquery")

PROJECT_ID = "kd-bq-sandbox-2026"
LOCATION = "US"

DATASETS = ["github_bronze", "github_silver", "github_marts"]

def setup_bigquery():
    client = bigquery.Client(project=PROJECT_ID)
    log.info(f"Connected to BigQuery client for project: {PROJECT_ID}")

    # 1. Create Datasets
    for ds_name in DATASETS:
        dataset_id = f"{PROJECT_ID}.{ds_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = LOCATION
        dataset.description = f"GitHub Telemetry Lakehouse - {ds_name} layer"
        try:
            client.create_dataset(dataset, exists_ok=True)
            log.info(f"Dataset verified/created: {dataset_id}")
        except Conflict:
            log.info(f"Dataset already exists: {dataset_id}")

    # 2. Create Bronze Partitioned Table with 7-Day Partition Expiration
    table_id = f"{PROJECT_ID}.github_bronze.raw_events"
    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED", description="Unique GitHub event ID"),
        bigquery.SchemaField("event_type", "STRING", mode="NULLABLE", description="GitHub event type e.g. PushEvent"),
        bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED", description="Unstructured JSON event string"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED", description="UTC timestamp when batch was ingested"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    # Day partitioning on ingested_at
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",
        expiration_ms=7 * 24 * 60 * 60 * 1000,  # 7 days in milliseconds
    )
    # Cluster by event_type
    table.clustering_fields = ["event_type"]
    table.description = "Bronze raw event logs partitioned by daily ingestion date with 7-day TTL"

    try:
        client.create_table(table, exists_ok=True)
        log.info(f"Bronze table verified/created: {table_id} (7-day partition TTL, clustered by event_type)")
    except Exception as e:
        log.error(f"Error creating bronze table: {e}")
        sys.exit(1)

    log.info("BigQuery environment bootstrap completed successfully.")

if __name__ == "__main__":
    setup_bigquery()
