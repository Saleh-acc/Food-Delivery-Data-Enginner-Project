{{ config(
    materialized = 'incremental',
    unique_key   = 'order_item_id'
) }}


SELECT
    sha2(CAST(oitm.order_item_id AS STRING), 256) AS order_item_key,
    oitm.order_item_id,
    oitm.order_id,
    m.menu_item_key,
    r.restaurant_key,
    c.customer_key,
    CAST(date_format(ord.placed_at_ts, 'yyyyMMdd') AS int) AS placed_date_key,
    CAST(date_format(ord.placed_at_ts, 'HHmm')     AS int) AS placed_time_key,
    oitm.quantity,
    oitm.line_total,
    oitm.unit_price,
    oitm.last_source_update_ts,
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'order_items') }} oitm
LEFT JOIN {{ source('silver', 'orders') }} ord
    ON ord.order_id = oitm.order_id
LEFT JOIN {{ ref('dim_menu_items') }} m
    ON  oitm.menu_item_id = m.menu_item_id
    AND ord.placed_at_ts >= m.eff_start_ts
    AND ord.placed_at_ts <  COALESCE(m.eff_end_ts, TIMESTAMP '9999-12-31')
LEFT JOIN {{ ref('dim_restaurants') }} r
    ON  ord.restaurant_id = r.restaurant_id
    AND ord.placed_at_ts >= r.eff_start_ts
    AND ord.placed_at_ts <  COALESCE(r.eff_end_ts, TIMESTAMP '9999-12-31')
LEFT JOIN {{ ref('dim_customers') }} c
    ON ord.customer_id = c.customer_id
--    AND ord.placed_at_ts >= r.eff_start_ts
--    AND ord.placed_at_ts <  COALESCE(r.eff_end_ts, TIMESTAMP '9999-12-31')
-- this: refers to the model's own table in gold layer
{% if is_incremental() %}
  WHERE oitm.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
{% endif %}