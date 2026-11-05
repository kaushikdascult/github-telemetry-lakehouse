{{ config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by=['event_type', 'is_off_peak_activity']
) }}

WITH silver_events AS (
    SELECT
        event_id,
        event_type,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        created_at_utc,
        original_tz_offset,
        ingested_at
    FROM {{ ref('stg_github_events') }}
    -- Enforce rolling 7-day analytical SLA window to align with Bronze partition TTL and prevent query bloat
    WHERE created_at_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)

SELECT
    event_id,
    event_type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    created_at_utc,
    DATE(created_at_utc) AS event_date,
    EXTRACT(HOUR FROM created_at_utc) AS event_hour_utc,
    FORMAT_DATE('%A', DATE(created_at_utc)) AS day_name,
    CASE WHEN EXTRACT(DAYOFWEEK FROM DATE(created_at_utc)) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    original_tz_offset,

    -- Global 'Follow-the-Sun' Regional Reference Timelines (Fixed Zone Estimates)
    DATETIME(created_at_utc, "America/New_York") AS amer_local_time,
    DATETIME(created_at_utc, "Europe/London")    AS emea_local_time,
    DATETIME(created_at_utc, "Asia/Singapore")   AS apac_local_time,
    DATETIME(created_at_utc, "Asia/Kolkata")     AS ist_local_time,

    -- Regional Reference Operating Hours Flags (09:00 - 17:00 in Reference Hubs)
    (EXTRACT(HOUR FROM DATETIME(created_at_utc, "America/New_York")) BETWEEN 9 AND 17) AS is_amer_core_hours,
    (EXTRACT(HOUR FROM DATETIME(created_at_utc, "Europe/London")) BETWEEN 9 AND 17)    AS is_emea_core_hours,
    (EXTRACT(HOUR FROM DATETIME(created_at_utc, "Asia/Singapore")) BETWEEN 9 AND 17)   AS is_apac_core_hours,

    -- Estimated Off-Peak Activity Flag (Late night 22:00 - 05:00 based on origin offset or UTC)
    -- Note: Reflects timestamp offset distribution; does not imply physical actor domicile
    CASE 
        WHEN original_tz_offset = 'Z' THEN
            (EXTRACT(HOUR FROM created_at_utc) >= 22 OR EXTRACT(HOUR FROM created_at_utc) < 5)
        ELSE
            (EXTRACT(HOUR FROM DATETIME(created_at_utc, original_tz_offset)) >= 22 
             OR EXTRACT(HOUR FROM DATETIME(created_at_utc, original_tz_offset)) < 5)
    END AS is_off_peak_activity,

    ingested_at

FROM silver_events
