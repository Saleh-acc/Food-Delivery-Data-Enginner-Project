{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id'
) }}



SELECT
    sha2(CAST(order_id AS STRING), 256) as order_key,
    order_id         ,
    ord.customer_id      ,
    r.restaurant_key    ,
    dri.driver_key        ,
    pz.zone_key  AS pickup_zone_key,
    dz.zone_key  AS dropoff_zone_key,
    CAST(date_format(ord.placed_at_ts,    'yyyyMMdd') AS int) AS placed_date_key,
    CAST(date_format(ord.delivered_at_ts, 'yyyyMMdd') AS int) AS delivered_date_key,
    CAST(date_format(ord.placed_at_ts,    'HHmm')     AS int) AS placed_time_key,
    CAST(date_format(ord.delivered_at_ts, 'HHmm')     AS int) AS delivered_time_key,
    ord.status           ,
    ord.subtotal          ,
    ord.delivery_fee     ,
    ord.service_fee      ,
    ord.discount         ,
    ord.tip              ,
    ord.total            ,
    -- durations in minutes (timestampdiff or unix_timestamp difference)
    (unix_timestamp(ord.ready_at_ts)     - unix_timestamp(ord.confirmed_at_ts)) / 60  AS prep_duration_min,
    (unix_timestamp(ord.picked_up_at_ts) - unix_timestamp(ord.ready_at_ts))     / 60  AS pickup_wait_min,
    (unix_timestamp(ord.delivered_at_ts) - unix_timestamp(ord.picked_up_at_ts)) / 60  AS delivery_duration_min,
    (unix_timestamp(ord.delivered_at_ts) - unix_timestamp(ord.placed_at_ts))    / 60  AS total_fulfillment_min,

    -- flags
    CASE WHEN ord.status = 'delivered' THEN 1 ELSE 0 END  AS is_delivered,
    CASE WHEN ord.status = 'cancelled' THEN 1 ELSE 0 END  AS is_cancelled,
    ord.placed_at_ts        ,
    ord.confirmed_at_ts     ,
    ord.ready_at_ts         ,
    ord.picked_up_at_ts     ,
    ord.delivered_at_ts     ,
    ord.cancelled_at_ts     ,
    ord.created_at_ts       ,
    ord.last_source_update_ts ,
    current_timestamp() AS last_refresh_ts
FROM {{ source('silver', 'orders') }} ord
LEFT JOIN {{ ref('dim_drivers') }} dri
    ON ord.driver_id =  dri.driver_id  
LEFT JOIN {{ ref('dim_zones') }} pz 
    ON ord.pickup_zone_id  = pz.zone_id
LEFT JOIN {{ ref('dim_zones') }} dz 
    ON ord.dropoff_zone_id = dz.zone_id
LEFT JOIN {{ ref('dim_restaurants') }} r
   ON  ord.restaurant_id = r.restaurant_id
   AND ord.placed_at_ts >= r.eff_start_ts
   AND ord.placed_at_ts <  COALESCE(r.eff_end_ts, TIMESTAMP '9999-12-31')

-- this: refers to the model's own table in gold layer
{% if is_incremental() %}
  WHERE ord.last_source_update_ts  > (SELECT MAX(g.last_source_update_ts) FROM {{ this }} g)
{% endif %}