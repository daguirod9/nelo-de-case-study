-- Silver Items Model
-- Transforms Bronze JSON files into silver_items table
-- Unnests items array and extracts item_params into columns

-- Create staging table from Bronze JSONs with unnested items
CREATE OR REPLACE TEMP TABLE stg_bronze_items AS
WITH bronze_raw AS (
    SELECT 
        message_id::VARCHAR AS message_id,
        body.items AS items_list
    FROM read_json_auto(
        '{{ bronze_path }}/**/*.json',
        maximum_object_size=10485760,
        ignore_errors=true
    )
    WHERE body.items IS NOT NULL
),
unnested_items AS (
    SELECT 
        message_id,
        unnest(items_list) AS item,
        ROW_NUMBER() OVER (PARTITION BY message_id) AS item_index
    FROM bronze_raw
)
SELECT 
    message_id,
    item_index,
    item.item_id AS item_id,
    item.item_name AS item_name,
    item.item_brand AS item_brand,
    item.item_variant AS item_variant,
    item.item_category AS item_category,
    item.item_category2 AS item_category2,
    item.item_category3 AS item_category3,
    item.item_category4 AS item_category4,
    item.item_category5 AS item_category5,
    TRY_CAST(item.price_in_usd AS DECIMAL(12,4)) AS price_in_usd,
    TRY_CAST(item.price AS DECIMAL(12,4)) AS price,
    COALESCE(TRY_CAST(item.quantity AS INTEGER), 1) AS quantity,
    TRY_CAST(item.item_revenue_in_usd AS DECIMAL(12,4)) AS item_revenue_in_usd,
    TRY_CAST(item.item_revenue AS DECIMAL(12,4)) AS item_revenue,
    TRY_CAST(item.item_refund_in_usd AS DECIMAL(12,4)) AS item_refund_in_usd,
    TRY_CAST(item.item_refund AS DECIMAL(12,4)) AS item_refund,
    item.coupon AS coupon,
    item.affiliation AS affiliation,
    item.location_id AS location_id,
    item.item_list_id AS item_list_id,
    item.item_list_name AS item_list_name,
    TRY_CAST(item.item_list_index AS INTEGER) AS item_list_index,
    item.promotion_id AS promotion_id,
    item.promotion_name AS promotion_name,
    item.creative_name AS creative_name,
    item.creative_slot AS creative_slot,
    item.item_params AS item_params_list
FROM unnested_items;

-- Unnest item_params into separate rows
CREATE OR REPLACE TEMP TABLE stg_item_params AS
SELECT 
    bi.message_id,
    bi.item_index,
    param.key AS param_key,
    COALESCE(
        param.value.string_value::VARCHAR,
        param.value.int_value::VARCHAR,
        param.value.float_value::VARCHAR,
        param.value.double_value::VARCHAR
    ) AS param_value,
    param.value.int_value AS param_int_value
FROM stg_bronze_items bi, unnest(bi.item_params_list) AS t(param)
WHERE bi.item_params_list IS NOT NULL;

-- Pivot item_params to columns
CREATE OR REPLACE TEMP TABLE stg_item_params_pivot AS
SELECT 
    message_id,
    item_index,
    MAX(CASE WHEN param_key = 'in_stock' THEN param_int_value END) AS in_stock,
    MAX(CASE WHEN param_key = 'discounts' THEN param_value END) AS discounts,
    MAX(CASE WHEN param_key IN ('discountt', 'discount', 'discount_amount') THEN param_value END) AS discount_amount,
    MAX(CASE WHEN param_key = 'totalPrice' THEN param_value END) AS total_price,
    MAX(CASE WHEN param_key = 'number_of_installments' THEN param_value END) AS number_of_installments,
    MAX(CASE WHEN param_key = 'installment_price' THEN param_value END) AS installment_price
FROM stg_item_params
GROUP BY message_id, item_index;

-- Insert into silver_items joining with silver_events and pivoted params
INSERT INTO silver_items (
    item_record_id,
    event_id,
    item_id,
    item_name,
    item_brand,
    item_variant,
    item_category,
    item_category2,
    item_category3,
    item_category4,
    item_category5,
    price_in_usd,
    price,
    quantity,
    item_revenue_in_usd,
    item_revenue,
    item_refund_in_usd,
    item_refund,
    coupon,
    affiliation,
    location_id,
    item_list_id,
    item_list_name,
    item_list_index,
    promotion_id,
    promotion_name,
    creative_name,
    creative_slot,
    in_stock,
    discounts,
    discount_amount,
    total_price,
    number_of_installments,
    installment_price
)
SELECT 
    gen_random_uuid()::VARCHAR AS item_record_id,
    se.event_id,
    bi.item_id,
    bi.item_name,
    bi.item_brand,
    bi.item_variant,
    bi.item_category,
    bi.item_category2,
    bi.item_category3,
    bi.item_category4,
    bi.item_category5,
    bi.price_in_usd,
    bi.price,
    bi.quantity,
    bi.item_revenue_in_usd,
    bi.item_revenue,
    bi.item_refund_in_usd,
    bi.item_refund,
    bi.coupon,
    bi.affiliation,
    bi.location_id,
    bi.item_list_id,
    bi.item_list_name,
    COALESCE(bi.item_list_index, bi.item_index::INTEGER) AS item_list_index,
    bi.promotion_id,
    bi.promotion_name,
    bi.creative_name,
    bi.creative_slot,
    pp.in_stock,
    pp.discounts,
    TRY_CAST(pp.discount_amount AS DECIMAL(12,4)) AS discount_amount,
    TRY_CAST(pp.total_price AS DECIMAL(12,4)) AS total_price,
    TRY_CAST(TRY_CAST(pp.number_of_installments AS DOUBLE) AS INTEGER) AS number_of_installments,
    pp.installment_price
FROM stg_bronze_items bi
INNER JOIN silver_events se ON se.message_id = bi.message_id
LEFT JOIN stg_item_params_pivot pp ON pp.message_id = bi.message_id AND pp.item_index = bi.item_index
WHERE se.event_id NOT IN (SELECT DISTINCT event_id FROM silver_items);
