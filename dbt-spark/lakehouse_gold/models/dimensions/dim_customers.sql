-- type of table 
{{ config(
    materialized = 'incremental',
    unique_key   = 'customer_id'
) }}

SELECT
    sha2(CAST(customer_id AS STRING), 256) AS customer_key,
    customer_id,
    email,
    full_name,
    prev_phone,
    phone,
    prev_city_id,
    city_id,
    default_address,
    is_active,
    CAST(DATE_FORMAT(signup_ts, 'yyyyMMdd') as int ) AS signup_date_key,
    CAST(DATE_FORMAT(signup_ts, 'HHmm') as int ) AS signup_time_key,
    signup_ts,
    created_at_ts,
    last_source_update_ts,
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'customers') }} s

-- this: refers to the model's own table in gold layer
{% if is_incremental() %}
  WHERE s.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
{% endif %}