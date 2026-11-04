# ADR-001: Architecture and Platform Choices

**Status**: Accepted  
**Project**: GitHub Telemetry Lakehouse (`github-telemetry-lakehouse`)  
**Target GCP Environment**: `kd-bq-sandbox-2026` (BigQuery Sandbox — Zero Billing / $0 Cost)  

---

## 1. Context

In modern distributed enterprises, engineering teams generate millions of asynchronous events across multiple time zones. Engineering leadership requires operational telemetry to measure cross-regional engineering velocity ("Follow-the-Sun" index) and detect unsustainable work patterns ("Midnight Oil" burnout indicator).

**Constraints**:
1. Zero cloud expenditure ($0 budget; expired trial accounts).
2. Local host disk conservation: Must avoid generating large local data buffers or duplicate Python virtual environments.
3. BigQuery Sandbox restrictions: No billing account attached; DML queries (`MERGE`, `DELETE`, `UPDATE`) are prohibited by Google Cloud policy in the free sandbox.

---

## 2. Decisions

### Decision 1: BigQuery Sandbox as the Lakehouse Engine
* **Choice**: Google BigQuery Sandbox (`kd-bq-sandbox-2026`).
* **Reasoning**: Provides 10 GB active storage and 1 TB/month free analytical query processing without credit card requirements. Natively parses nested JSON payloads and delivers sub-second analytical aggregations.
* **Trade-off**: Tables default to 60-day expiration, which is entirely acceptable because our rolling business SLA window is strictly 7 days.

### Decision 2: In-Memory Streaming Ingestion (Zero Local Disk Accumulation)
* **Choice**: Python in-memory `io.StringIO` buffers streaming NDJSON directly to BigQuery Bronze via `client.load_table_from_file()`.
* **Reasoning**: Eliminates local file spooling. Prevents disk exhaustion on local storage while maintaining fast micro-batch ingest capability.

### Decision 3: DDL & View Deduplication over DML MERGE (The Sandbox Protocol)
* **Choice**: Materialize `stg_github_events` as a SQL `view` with analytical deduplication (`ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC)`), and `fct_global_developer_activity` as a CTAS partitioned table.
* **Reasoning**: In BigQuery Sandbox without a billing account, DML statements (`MERGE`) trigger `Billing has not been enabled for this project. DML queries are not allowed in the free tier`. Utilizing DDL (`CREATE VIEW` and `CREATE OR REPLACE TABLE`) bypasses the DML lock completely while maintaining 100% schema integrity and idempotency.

### Decision 4: 7-Day Automated Partition TTL & Rolling SLA Window
* **Choice**: BigQuery native table partition expiration (`expiration_ms = 7 days`) on Bronze `raw_events`, coupled with `WHERE created_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)` on Gold marts.
* **Reasoning**: Automatically purges historical data older than 7 days, capping storage consumption at ~400 MB (< 4% of the 10 GB Sandbox limit).

### Decision 5: Leveraging Existing Python Environment (`my_python312_env`)
* **Choice**: Reusing the pre-installed `/home/datadask/my_python312_env` rather than creating a duplicate virtual environment.
* **Reasoning**: Avoids duplicating 500+ MB of system dependencies, keeping the local machine lean.

### Decision 6: OAuth Application Default Credentials (ADC)
* **Choice**: Authenticating via `gcloud auth application-default login` for both Python and dbt Core.
* **Reasoning**: Eliminates static service account JSON keys, complying with enterprise zero-trust security policies (`constraints/iam.disableServiceAccountKeyCreation`).

---

## 3. Consequences

* Total cloud cost: **$0.00**.
* Local disk footprint: **Zero local data accumulation**.
* End-to-end execution runtime: **~40 seconds** for Ingestion $\to$ dbt Run $\to$ 12 Data Tests.
* Seamless BI connectivity: Connects directly to free Google Looker Studio.
