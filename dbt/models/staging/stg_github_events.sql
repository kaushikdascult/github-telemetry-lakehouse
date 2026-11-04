{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        event_id,
        event_type,
        raw_payload,
        ingested_at
    FROM {{ source('github_bronze', 'raw_events') }}
    WHERE event_id IS NOT NULL
),

deduped_source AS (
    -- Deduplicate events using ROW_NUMBER on event_id across micro-batches
    SELECT
        event_id,
        event_type,
        raw_payload,
        ingested_at,
        ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY ingested_at DESC) AS row_num
    FROM raw_source
),

parsed_events AS (
    SELECT
        event_id,
        event_type,
        SAFE_CAST(JSON_VALUE(raw_payload, '$.actor.id') AS INT64) AS actor_id,
        JSON_VALUE(raw_payload, '$.actor.login') AS actor_login,
        SAFE_CAST(JSON_VALUE(raw_payload, '$.repo.id') AS INT64) AS repo_id,
        JSON_VALUE(raw_payload, '$.repo.name') AS repo_name,
        SAFE_CAST(JSON_VALUE(raw_payload, '$.created_at') AS TIMESTAMP) AS created_at_utc,
        COALESCE(REGEXP_EXTRACT(JSON_VALUE(raw_payload, '$.created_at'), r'([+-]\d{2}:\d{2}|Z)$'), 'Z') AS original_tz_offset,
        ingested_at
    FROM deduped_source
    WHERE row_num = 1
)

SELECT
    event_id,
    event_type,
    actor_id,
    COALESCE(actor_login, 'unknown') AS actor_login,
    repo_id,
    COALESCE(repo_name, 'unknown') AS repo_name,
    created_at_utc,
    original_tz_offset,
    ingested_at
FROM parsed_events
WHERE created_at_utc IS NOT NULL
