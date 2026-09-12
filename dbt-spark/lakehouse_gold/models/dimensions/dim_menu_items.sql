-- type of table 
{{ config(
    materialized = 'incremental',
    unique_key   = 'menu_item_key'
) }}


SELECT
    sha2(CONCAT(CAST(menu_item_id AS STRING), CAST(eff_start_ts AS STRING)), 256) as menu_item_key,
    menu_item_id            ,
    restaurant_id           ,
    name                    ,
    category                ,
    price                   ,
    is_available            ,
    created_at_ts           ,
    eff_start_ts            ,
    eff_end_ts              ,
    is_current              ,
    last_source_update_ts   ,
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'menu_items') }} s

-- this: refers to the model's own table in gold layer
{% if is_incremental() %}
  WHERE s.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
{% endif %}