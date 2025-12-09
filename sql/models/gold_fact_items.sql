-- Gold Fact Event Items Model
-- Transforms Silver items into fact_event_items with derived fields

INSERT INTO fact_event_items (
    event_item_id,
    event_id,
    item_id,
    item_name,
    item_list_name,
    item_list_id,
    item_category,
    item_brand,
    price,
    total_price,
    quantity,
    position_in_list,
    has_discount,
    discount_amount,
    in_stock
)
SELECT 
    gen_random_uuid()::VARCHAR AS event_item_id,
    si.event_id,
    si.item_id,
    si.item_name,
    si.item_list_name,
    si.item_list_id,
    si.item_category,
    si.item_brand,
    si.price,
    si.total_price,
    si.quantity,
    si.item_list_index AS position_in_list,
    -- Derive has_discount flag
    CASE 
        WHEN si.discounts IS NOT NULL 
             OR (si.discount_amount IS NOT NULL AND si.discount_amount > 0)
        THEN TRUE
        ELSE FALSE
    END AS has_discount,
    si.discount_amount,
    -- Convert in_stock to boolean
    CASE 
        WHEN si.in_stock = 1 THEN TRUE
        WHEN si.in_stock = 0 THEN FALSE
        ELSE NULL
    END AS in_stock
FROM silver_items si
INNER JOIN fact_events fe ON fe.event_id = si.event_id
WHERE si.event_id NOT IN (SELECT DISTINCT event_id FROM fact_event_items);

