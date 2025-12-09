-- Gold Dimension Users Model
-- Extracts unique users and manages SCD Type 2 slowly changing dimension

-- Update last_seen_at and last_platform for existing users
UPDATE dim_users 
SET 
    last_seen_at = CURRENT_TIMESTAMP,
    last_platform = (
        SELECT platform 
        FROM silver_events se 
        WHERE se.user_id = dim_users.user_id 
        ORDER BY event_timestamp DESC 
        LIMIT 1
    ),
    total_sessions = total_sessions + (
        SELECT COUNT(DISTINCT session_id)
        FROM fact_events fe
        WHERE fe.user_id = dim_users.user_id
        AND fe.processed_at > dim_users.last_seen_at
    )
WHERE user_id IN (
    SELECT DISTINCT user_id 
    FROM silver_events
)
AND is_current = TRUE;

-- Insert new users that don't exist yet
INSERT INTO dim_users (
    user_sk,
    user_id,
    first_platform,
    last_platform,
    first_seen_at,
    last_seen_at,
    total_sessions,
    is_current
)
WITH new_users AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        platform
    FROM silver_events
    ORDER BY user_id, event_timestamp ASC  -- Take first platform for each user
)
SELECT 
    gen_random_uuid()::VARCHAR AS user_sk,
    nu.user_id,
    nu.platform AS first_platform,
    (
        SELECT platform 
        FROM silver_events se 
        WHERE se.user_id = nu.user_id 
        ORDER BY event_timestamp DESC 
        LIMIT 1
    ) AS last_platform,
    CURRENT_TIMESTAMP AS first_seen_at,
    CURRENT_TIMESTAMP AS last_seen_at,
    (
        SELECT COUNT(DISTINCT session_id)
        FROM fact_events fe
        WHERE fe.user_id = nu.user_id
    ) AS total_sessions,
    TRUE AS is_current
FROM new_users nu
WHERE nu.user_id NOT IN (
    SELECT user_id FROM dim_users WHERE is_current = TRUE
);

