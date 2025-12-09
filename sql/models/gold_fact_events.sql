-- Gold Fact Events Model
-- Transforms Silver events into fact_events with session derivation
-- Session logic: New session if >30 minutes since last event for same user

-- Create staging with session calculation using window functions
CREATE OR REPLACE TEMP TABLE stg_fact_events AS
WITH events_with_lag AS (
    SELECT 
        event_id,
        event_timestamp,
        user_id,
        event_name,
        platform,
        message_id AS raw_message_id,
        CAST(event_timestamp AS DATE) AS event_date,
        EXTRACT(HOUR FROM event_timestamp) AS event_hour,
        LAG(event_timestamp) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
        ) AS prev_event_timestamp
    FROM silver_events
    WHERE event_id NOT IN (SELECT event_id FROM fact_events)
),
events_with_session_start AS (
    SELECT 
        *,
        CASE 
            WHEN prev_event_timestamp IS NULL 
                 OR (event_timestamp - prev_event_timestamp) > INTERVAL '30 minutes'
            THEN 1
            ELSE 0
        END AS is_session_start
    FROM events_with_lag
),
events_with_session_id AS (
    SELECT 
        *,
        SUM(is_session_start) OVER (
            PARTITION BY user_id 
            ORDER BY event_timestamp
            ROWS UNBOUNDED PRECEDING
        ) AS session_number
    FROM events_with_session_start
)
SELECT 
    event_id,
    event_timestamp,
    user_id,
    event_name,
    platform,
    -- Generate session_id as hash of user_id + session_number
    SUBSTRING(MD5(user_id || '_' || session_number::VARCHAR), 1, 16) AS session_id,
    event_date::VARCHAR AS event_date,
    event_hour::INTEGER AS event_hour,
    raw_message_id
FROM events_with_session_id;

-- Insert new fact events
INSERT INTO fact_events (
    event_id,
    event_timestamp,
    user_id,
    event_name,
    platform,
    session_id,
    event_date,
    event_hour,
    raw_message_id,
    processed_at
)
SELECT 
    event_id,
    event_timestamp,
    user_id,
    event_name,
    platform,
    session_id,
    event_date,
    event_hour,
    raw_message_id,
    CURRENT_TIMESTAMP AS processed_at
FROM stg_fact_events;

