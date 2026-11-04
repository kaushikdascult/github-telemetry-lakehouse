-- Singular test: Verify no event has a timestamp in the future (allowing 10 min clock drift)
SELECT
    event_id,
    created_at_utc
FROM {{ ref('stg_github_events') }}
WHERE created_at_utc > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
