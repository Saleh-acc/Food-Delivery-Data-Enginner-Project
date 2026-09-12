{{ config(
    materialized = 'table'
) }}

SELECT
    sha2(CAST(zone_id AS STRING), 256) as zone_key,
    zone_id,
    city_id,
    zone_name,
    is_active,
    current_timestamp() as last_refresh_ts
FROM {{ source('silver', 'zones')}}