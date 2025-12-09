-- Silver Events Model
-- Transforms Bronze JSON files into silver_events table
-- DuckDB auto-parses JSON into STRUCTs, so we use dot notation

CREATE OR REPLACE TEMP TABLE stg_bronze_events AS
SELECT 
    message_id::VARCHAR AS message_id,
    body.user_id::VARCHAR AS user_id,
    body.event_name::VARCHAR AS event_name,
    body.platform::VARCHAR AS platform,
    body.event_timestamp::BIGINT AS event_timestamp_micros,
    body.replay_timestamp::VARCHAR AS replay_timestamp_str,
    body.items AS items_array
FROM read_json_auto(
    '{{ bronze_path }}/**/*.json',
    maximum_object_size=10485760,
    ignore_errors=true
);

-- Insert new events into silver_events (incremental by message_id)
INSERT INTO silver_events (
    event_id,
    message_id,
    event_timestamp,
    event_timestamp_micros,
    user_id,
    event_name,
    platform,
    replay_timestamp,
    received_at
)
SELECT 
    gen_random_uuid()::VARCHAR AS event_id,
    message_id,
    to_timestamp(event_timestamp_micros / 1000000.0) AS event_timestamp,
    event_timestamp_micros,
    user_id,
    event_name,
    platform,
    TRY_CAST(REPLACE(replay_timestamp_str, 'Z', '+00:00') AS TIMESTAMP) AS replay_timestamp,
    CURRENT_TIMESTAMP AS received_at
FROM stg_bronze_events
WHERE message_id NOT IN (SELECT message_id FROM silver_events);
