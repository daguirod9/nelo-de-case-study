-- Gold Dimension Items Model
-- Extracts unique items and manages SCD Type 2 slowly changing dimension

-- Update last_seen_at for existing items
UPDATE dim_items 
SET last_seen_at = CURRENT_TIMESTAMP
WHERE item_id IN (
    SELECT DISTINCT item_id 
    FROM silver_items 
    WHERE item_id IS NOT NULL
)
AND is_current = TRUE;

-- Insert new items that don't exist yet
INSERT INTO dim_items (
    item_sk,
    item_id,
    item_name,
    item_brand,
    item_category,
    item_category2,
    item_category3,
    item_category4,
    item_category5,
    first_seen_at,
    last_seen_at,
    is_current
)
WITH new_items AS (
    SELECT DISTINCT ON (item_id)
        item_id,
        item_name,
        item_brand,
        item_category,
        item_category2,
        item_category3,
        item_category4,
        item_category5
    FROM silver_items
    WHERE item_id IS NOT NULL
    ORDER BY item_id, item_record_id DESC  -- Take latest record for each item
)
SELECT 
    gen_random_uuid()::VARCHAR AS item_sk,
    item_id,
    item_name,
    item_brand,
    item_category,
    item_category2,
    item_category3,
    item_category4,
    item_category5,
    CURRENT_TIMESTAMP AS first_seen_at,
    CURRENT_TIMESTAMP AS last_seen_at,
    TRUE AS is_current
FROM new_items
WHERE item_id NOT IN (
    SELECT item_id FROM dim_items WHERE is_current = TRUE
);

