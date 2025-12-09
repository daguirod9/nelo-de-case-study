SELECT 
    fi.event_item_id,
    fi.event_id,
    fi.item_id,
    fi.item_name,
    fi.item_list_name,
    fi.item_list_id,
    fi.item_category,
    fi.item_brand,
    fi.price,
    fi.total_price,
    fi.quantity,
    fi.position_in_list,
    fi.has_discount,
    fi.discount_amount,
    fi.in_stock,
    fe.event_timestamp,
    fe.user_id,
    fe.session_id,
    fe.event_date,
    fe.event_hour,
    fe.processed_at as processed_at_fact_events
FROM fact_event_items fi
INNER JOIN fact_events fe ON fi.event_id = fe.event_id
WHERE fi.item_list_name IS NOT NULL
