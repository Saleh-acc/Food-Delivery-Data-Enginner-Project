-- type of table 
{{ config(
    materialized = 'table',
   
) }}




SELECT
    sha2(CONCAT(CAST(restaurant_id AS STRING), CAST(eff_start_ts AS STRING)), 256) as restaurant_key,
    restaurant_id        , 
    name                 ,
    cuisine_type         ,
    city_id              ,
    zone_id              ,
    address              ,
    rating_avg           , 
    is_active            ,
    is_current           ,
    eff_start_ts         ,
    eff_end_ts           ,
    onboarded_at         ,
    created_at_ts        ,
    last_source_update_ts, 
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'restaurants') }} s

-- this: refers to the model's own table in gold layer
-- {% if is_incremental() %}
--   WHERE s.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
-- {% endif %}