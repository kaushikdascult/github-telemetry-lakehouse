"""
GitHub Telemetry Ingestion: Fetch Live Event Micro-Batches & Stream to BigQuery Bronze.

Target: kd-bq-sandbox-2026.github_bronze.raw_events
Zero Disk Footprint: Uses in-memory StringIO buffers; no local files created or accumulated.
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from google.cloud import bigquery
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("fetch_and_push_bq")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "kd-bq-sandbox-2026")
DATASET_ID = os.getenv("BQ_BRONZE_DATASET", "github_bronze")
TABLE_ID = os.getenv("BQ_BRONZE_TABLE", "raw_events")
GITHUB_API_URL = "https://api.github.com/events"


def fetch_github_events() -> list:
    """Fetch recent public events from the GitHub REST API."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "GitHub-Telemetry-Lakehouse-Pipeline/1.0",
    }
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
        log.info("Using authenticated GitHub API request with Bearer token.")
    else:
        log.info("Requesting GitHub API anonymously (rate limit: 60 req/hr).")

    response = requests.get(GITHUB_API_URL, headers=headers, timeout=15)
    if response.status_code != 200:
        log.error(f"GitHub API returned error {response.status_code}: {response.text}")
        response.raise_for_status()

    events = response.json()
    if not isinstance(events, list):
        raise ValueError(f"Expected list of events, received: {type(events)}")

    log.info(f"Retrieved {len(events)} events from GitHub API.")
    return events


def stream_to_bigquery(events: list) -> int:
    """Stream events to BigQuery Bronze table via in-memory newline-delimited JSON."""
    if not events:
        log.warning("No events to ingest.")
        return 0

    client = bigquery.Client(project=PROJECT_ID)
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Format rows in-memory
    buffer = io.StringIO()
    row_count = 0
    for event in events:
        event_id = str(event.get("id"))
        if not event_id or event_id == "None":
            continue

        record = {
            "event_id": event_id,
            "event_type": event.get("type", "Unknown"),
            "raw_payload": json.dumps(event),
            "ingested_at": ingested_at,
        }
        buffer.write(json.dumps(record) + "\n")
        row_count += 1

    buffer.seek(0)
    bytes_data = buffer.getvalue().encode("utf-8")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("raw_payload", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )

    log.info(f"Uploading {row_count} records ({len(bytes_data)} bytes) to {full_table_id}...")
    load_job = client.load_table_from_file(
        io.BytesIO(bytes_data),
        full_table_id,
        job_config=job_config,
    )

    load_job.result()  # Wait for job completion
    log.info(f"Load job {load_job.job_id} completed successfully. Appended {row_count} rows.")
    return row_count


def main():
    events = fetch_github_events()
    stream_to_bigquery(events)


if __name__ == "__main__":
    main()
