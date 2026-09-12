CREATE NAMESPACE IF NOT EXISTS lake.silver;

CREATE OR REPLACE TABLE  lake.silver.watermarks (
    table_name        STRING,
    last_watermark    TIMESTAMP,
    updated_at        TIMESTAMP
)
USING iceberg;


CREATE OR REPLACE TABLE lake.silver.orders (
    order_id         STRING,
    customer_id      STRING,
    restaurant_id    STRING,
    driver_id        STRING,
    pickup_zone_id   STRING,
    dropoff_zone_id  STRING,
    status           STRING,
    subtotal         DECIMAL ,
    delivery_fee     DECIMAL,
    service_fee      DECIMAL,
    discount         DECIMAL,
    tip              DECIMAL,
    total            DECIMAL,
    placed_at_ts        TIMESTAMP,
    confirmed_at_ts     TIMESTAMP,
    ready_at_ts         TIMESTAMP,
    picked_up_at_ts     TIMESTAMP,
    delivered_at_ts     TIMESTAMP,
    cancelled_at_ts     TIMESTAMP,
    created_at_ts       TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(placed_at_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.order_items (
    order_item_id         STRING,
    order_id              STRING,
    menu_item_id          STRING,
    quantity              INT,
    unit_price            DECIMAL,
    line_total            DECIMAL,
    created_at            TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.payments (
    payment_id           STRING,
    order_id             STRING,
    payment_method       STRING,
    amount               DECIMAL,
    status               STRING,
    processor_ref        STRING,
    paid_at              TIMESTAMP,
    created_at           TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(paid_at), status)
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.order_status_events (
    event_id             STRING,
    order_id             STRING,
    from_status          STRING,
    to_status            STRING,
    event_ts             TIMESTAMP,
    actor_type           STRING,
    actor_id             STRING,
    notes                STRING,
    created_at           TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at), to_status)
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.reviews (
    review_id            STRING,
    order_id             STRING,
    customer_id          STRING,
    restaurant_id        STRING,
    driver_id            STRING,
    food_rating          DECIMAL,
    delivery_rating      DECIMAL,
    comment              STRING,
    submitted_at         TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(submitted_at))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

-- /opt/spark-apps/ddl/silver_tables.sql
CREATE OR REPLACE TABLE   lake.silver.menu_items (
    menu_item_id            STRING,
    restaurant_id           STRING,
    name                    STRING,
    category                STRING,
    price                   DECIMAL(8,2),
    is_available            BOOLEAN,
    created_at_ts           TIMESTAMP,
    eff_start_ts            TIMESTAMP,
    eff_end_ts              TIMESTAMP,
    is_current              BOOLEAN,
    last_source_update_ts   TIMESTAMP,
    last_refresh_ts    TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE TABLE IF NOT EXISTS   lake.silver.drivers (
        driver_id             STRING,
        full_name             STRING,
        phone                 STRING,
        prev_vehicle_type     STRING,
        vehicle_type          STRING,
        prev_city_id          STRING,
        city_id               STRING,
        status                STRING,
        onboarded_at_ts       TIMESTAMP,
        is_active             BOOLEAN,
        created_at_ts         TIMESTAMP,
        last_refresh_ts       TIMESTAMP,
        last_source_update_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (city_id)
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);  

-- batch 
CREATE OR REPLACE TABLE lake.silver.customers (
    customer_id        STRING, 
    email              STRING,
    full_name          STRING,
    prev_phone         STRING,
    phone              STRING,
    prev_city_id       STRING,
    city_id            STRING, 
    default_address    STRING,
    is_active          BOOLEAN,
    signup_ts     TIMESTAMP,
    created_at_ts      TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg 
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);


CREATE OR REPLACE TABLE lake.silver.restaurants (
    restaurant_id        STRING, 
    name                 STRING,
    cuisine_type         STRING,
    city_id              STRING,
    zone_id              STRING,
    address              STRING,
    rating_avg           DECIMAL, 
    is_active            BOOLEAN,
    is_current           BOOLEAN,
    eff_start_ts       TIMESTAMP,
    eff_end_ts         TIMESTAMP,
    onboarded_at         TIMESTAMP,
    created_at_ts        TIMESTAMP,
    last_refresh_ts TIMESTAMP,
    last_source_update_ts TIMESTAMP
)
USING iceberg 
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.zones (
    zone_id              STRING, 
    city_id              STRING,
    zone_name            STRING,
    is_active            BOOLEAN,
    created_at_ts        TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg 
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE OR REPLACE TABLE lake.silver.cities (
    city_id              STRING, 
    city_name            STRING,
    country_code         STRING,
    timezone             STRING,
    created_at_ts        TIMESTAMP,
    last_source_update_ts TIMESTAMP,
    last_refresh_ts TIMESTAMP
)
USING iceberg 
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);