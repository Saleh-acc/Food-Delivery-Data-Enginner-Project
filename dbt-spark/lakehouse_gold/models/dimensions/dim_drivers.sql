-- type of table 
{{ config(
    materialized = 'incremental',
    unique_key   = 'driver_id'
) }}

SELECT
    sha2(CAST(driver_id AS STRING), 256) AS driver_key,
    driver_id            ,
    full_name            ,
    phone                ,
    prev_vehicle_type    ,
    vehicle_type         ,
    prev_city_id         ,
    city_id              ,
    status               ,
    onboarded_at_ts      ,
    is_active            ,
    created_at_ts        ,
    last_source_update_ts,
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'drivers') }} s

-- this: refers to the model's own table in gold layer
{% if is_incremental() %}
  WHERE s.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
{% endif %}