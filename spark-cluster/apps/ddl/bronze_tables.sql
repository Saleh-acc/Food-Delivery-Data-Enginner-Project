CREATE NAMESPACE IF NOT EXISTS lake.bronze;


CREATE TABLE IF NOT EXISTS lake.bronze.orders (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);

CREATE TABLE IF NOT EXISTS lake.bronze.order_items (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);CREATE TABLE IF NOT EXISTS lake.bronze.payments (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);CREATE TABLE IF NOT EXISTS lake.bronze.order_status_events (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);CREATE TABLE IF NOT EXISTS lake.bronze.reviews (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);CREATE TABLE IF NOT EXISTS lake.bronze.drivers (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);CREATE TABLE IF NOT EXISTS lake.bronze.menu_items (
    operation         STRING,
    source_ts_ms      BIGINT,
    source_lsn        BIGINT,
    source_txid       BIGINT,
    source_table      STRING,
    source_snapshot   STRING,
    before_payload    STRING,
    after_payload     STRING,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   TIMESTAMP,
    ingestion_ts      TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_ts))
TBLPROPERTIES (
    'format-version'                  = '2',
    'write.format.default'            = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes'    = '134217728'
);



-- Batches

CREATE TABLE IF NOT EXISTS lake.bronze.customers (
            operation         STRING,
            source_ts_ms      BIGINT,
            source_lsn        BIGINT,
            source_txid       BIGINT,
            source_table      STRING,
            source_snapshot   STRING,
            before_payload    STRING,
            after_payload     STRING,
            kafka_topic       STRING,
            kafka_partition   INT,
            kafka_offset      BIGINT,
            kafka_timestamp   TIMESTAMP,
            ingestion_ts      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_ts))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        );


CREATE TABLE IF NOT EXISTS lake.bronze.cities (
            operation         STRING,
            source_ts_ms      BIGINT,
            source_lsn        BIGINT,
            source_txid       BIGINT,
            source_table      STRING,
            source_snapshot   STRING,
            before_payload    STRING,
            after_payload     STRING,
            kafka_topic       STRING,
            kafka_partition   INT,
            kafka_offset      BIGINT,
            kafka_timestamp   TIMESTAMP,
            ingestion_ts      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_ts))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        );


CREATE TABLE IF NOT EXISTS lake.bronze.zones (
            operation         STRING,
            source_ts_ms      BIGINT,
            source_lsn        BIGINT,
            source_txid       BIGINT,
            source_table      STRING,
            source_snapshot   STRING,
            before_payload    STRING,
            after_payload     STRING,
            kafka_topic       STRING,
            kafka_partition   INT,
            kafka_offset      BIGINT,
            kafka_timestamp   TIMESTAMP,
            ingestion_ts      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_ts))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        );
CREATE TABLE IF NOT EXISTS lake.bronze.restaurants (
            operation         STRING,
            source_ts_ms      BIGINT,
            source_lsn        BIGINT,
            source_txid       BIGINT,
            source_table      STRING,
            source_snapshot   STRING,
            before_payload    STRING,
            after_payload     STRING,
            kafka_topic       STRING,
            kafka_partition   INT,
            kafka_offset      BIGINT,
            kafka_timestamp   TIMESTAMP,
            ingestion_ts      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingestion_ts))
        TBLPROPERTIES (
            'format-version'                  = '2',
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'    = '134217728'
        );


-- batch trackers
CREATE TABLE IF NOT EXISTS lake.bronze.batches_tracker (
             id STRING,
             job_name STRING,
             batch_number INTEGER,
             lower_bound INTEGER,
             upper_bound INTEGER,
             status STRING,
             rows_processed BIGINT,
             started_at_ts TIMESTAMP,
             completed_at_ts TIMESTAMP,
             error_message STRING,
             generated_at_ts TIMESTAMP 
         )
         USING iceberg
         PARTITIONED BY (job_name)
         TBLPROPERTIES (
        'format-version'                  = '2',
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.target-file-size-bytes'    = '134217728'
                        );