import warnings
from pyspark.sql import  Window
from pyspark.sql.functions import * #type:ignore
from pyspark.sql.types import *
import pyspark.sql.dataframe
import pandas as pd
import sys
import argparse
from spark_jobs.shared.measure_running import run_func
from spark_jobs.shared.spark_session import  get_spark_streaming
# Configure pandas to show ful output without truncation
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Don't truncate columns content
pd.set_option('display.width', None)        # Use full width
import time
from functools import partial
print("Libraries imported successfully")
warnings.filterwarnings("ignore")
from spark_jobs.ddl.tables_schema import  (
                                        orders_schema                             
                                )
from spark_jobs.shared.watermark import  update_watermark_table
from spark_jobs.shared.streaming_utils import flatten_kafka_payload

def initialize_read_strem(spark:pyspark.sql.session.SparkSession):
    return (
        spark.readStream
        .format("iceberg")
        #.option("stream-from-timestamp", "0")   #  start from the beginning
        .option("streaming-max-files-per-micro-batch", "50")

        .load("lake.bronze.orders")
    )

def transform(df_raw: pyspark.sql.DataFrame, 
              batch_id:int, 
              schema:pyspark.sql.types.StructType, 
              table_name: str = 'orders') -> pyspark.sql.DataFrame:
    """
    WHY separate transform:
    Flattens Kafka payload and deduplicates by LSN (last write wins per customer).
    Pure transformation — no I/O, no side effects.
    Safe to test independently with mock data.
    
    Returns None if no new data to process.
    """
    type(df_raw)
    print(f"[{table_name}] Transforming, table type: FACT, CDC? Yes, transforming type: UPSERT")
    
    df_flatern = flatten_kafka_payload(df = df_raw,
                                                       table_schema = schema )
        
    # remove the duplicates rows 
    window = Window.partitionBy("order_id").orderBy(col("source_lsn").desc())

    # remove duplicate rows 
    df_flattened_partitioned = df_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
    max_lsn = df_flattened_partitioned.agg({"source_lsn": "max"}).collect()[0][0] if "source_lsn" in df_flattened_partitioned.columns else None
    
    print(f"[{table_name}] batch {batch_id}:"
              f"max_source_lsn={max_lsn}")
    df_flattened_partitioned = df_flattened_partitioned.select(
                "order_id",
                "customer_id",
                "restaurant_id",
                "driver_id",
                "pickup_zone_id",
                "dropoff_zone_id",
                "status",
                "subtotal" ,
                "delivery_fee",
                "service_fee",
                "discount",  
                "tip",
                "total",
                    col("placed_at").alias("placed_at_ts"), 
                    col("confirmed_at").alias("confirmed_at_ts"),
                    col("ready_at").alias("ready_at_ts"),
                    col("picked_up_at").alias("picked_up_at_ts"),
                    col("delivered_at").alias("delivered_at_ts"),
                    col("cancelled_at").alias("cancelled_at_ts"),
                    col("created_at").alias("created_at_ts"),
                    col("updated_at").alias("updated_at_ts")
    )

    #print(f"Transformed {df_flattened_partitioned.count()} records for {table_name}")
    return df_flattened_partitioned


def load_to_silver(df_transformed:pyspark.sql.DataFrame,
                   table_name: str = 'orders'):
    """
    WHY separate load:
    Executes the MERGE INTO silver table. Isolated so if the merge fails,
    we don't re-read bronze or re-transform — we just retry the merge.
    Watermark is only updated after a successful merge — guarantees
    no data is silently skipped on failure.
    """
    if df_transformed is None:
        print(f"[{table_name}] No data to load - skipping")
        return
    
    spark = df_transformed.sparkSession
    df_transformed.createOrReplaceTempView("silver_orders_updates")
        # Compute a REAL progress marker before the MERGE
    
    
    try:

       
        source = spark.table("silver_orders_updates").alias("s")
        target = spark.table("lake.silver.orders").alias("t")
        updates = (
            source
            .join(target, on="order_id", how="left")
            .withColumn(
                "action",
                when(col("t.order_id").isNotNull(), "update")
                .otherwise("insert")
            )
        )

        # if the load is havy use .cache() then .persist to release  the memory 
        print(f"[{table_name}] total of processed rows:")
        updates.groupBy("action").count().show()
        spark.sql("""
            MERGE INTO lake.silver.orders od
            USING silver_orders_updates s
            ON od.order_id = s.order_id
            
            WHEN MATCHED THEN UPDATE SET
                od.customer_id           = s.customer_id,
                od.restaurant_id         = s.restaurant_id,
                od.driver_id             = s.driver_id,
                od.pickup_zone_id        = s.pickup_zone_id,
                od.dropoff_zone_id       = s.dropoff_zone_id,
                od.status                = s.status,
                od.subtotal              = s.subtotal,
                od.delivery_fee          = s.delivery_fee,
                od.service_fee           = s.service_fee,
                od.discount              = s.discount,
                od.tip                   = s.tip,
                od.total                 = s.total,
                od.placed_at_ts          = s.placed_at_ts,
                od.confirmed_at_ts       = s.confirmed_at_ts,
                od.ready_at_ts           = s.ready_at_ts,
                od.picked_up_at_ts       = s.picked_up_at_ts,
                od.delivered_at_ts       = s.delivered_at_ts,
                od.cancelled_at_ts       = s.cancelled_at_ts,
                od.created_at_ts         = s.created_at_ts,
                od.last_source_update_ts = s.updated_at_ts,        
                od.last_refresh_ts  = CURRENT_TIMESTAMP
            
            WHEN NOT MATCHED THEN INSERT (
                order_id,
                customer_id,
                restaurant_id,
                driver_id,
                pickup_zone_id,
                dropoff_zone_id,
                status,
                subtotal,
                delivery_fee,
                service_fee,
                discount,
                tip,
                total,
                placed_at_ts,
                confirmed_at_ts,
                ready_at_ts,
                picked_up_at_ts,
                delivered_at_ts,
                cancelled_at_ts,
                created_at_ts,
                last_source_update_ts,
                last_refresh_ts
            )
            VALUES (
                s.order_id,
                s.customer_id,
                s.restaurant_id,
                s.driver_id,
                s.pickup_zone_id,
                s.dropoff_zone_id,
                s.status,
                s.subtotal,
                s.delivery_fee,
                s.service_fee,
                s.discount,
                s.tip,
                s.total,
                s.placed_at_ts,
                s.confirmed_at_ts,
                s.ready_at_ts,
                s.picked_up_at_ts,
                s.delivered_at_ts,
                s.cancelled_at_ts,
                s.created_at_ts,
                s.updated_at_ts,
                CURRENT_TIMESTAMP          -- last_refresh_ts
            )
            """)
        print(f"[{table_name}] MERGE orders data from bronze --> silver: successed")

        
        # check remaining time after each batch
        elapsed = time.time() - start_time
        remaining = args.duration - elapsed
        print(f"[{table_name}] Remaining: {remaining}")
    except Exception as e:
        raise Exception(f"Error occred in function merge the {table_name.replace('_', ' ')} rows", e)
        
    
    # WHY watermark update is here and not in transform:
    # watermark must only advance after a successful write to silver.
    # If merge fails, watermark stays at previous value so next run
    # re-processes the same bronze records — no data loss.
    update_watermark_table(table_name=table_name)


def transform_then_load(micro_df:pyspark.sql.DataFrame,  
                        batch_id:int, 
                        schema:pyspark.sql.types.StructType,
                        table_name:str
                        ):
    
    df_transformed = transform(df_raw = micro_df,
                               batch_id=batch_id,
                               schema=schema,
                               table_name=table_name
                                   )
    
    load_to_silver(
                    df_transformed=df_transformed,
                    table_name=table_name
                )

if '__main__' == __name__:
    global start_time
    global duration
     
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, required=True, default='30')
    parser.add_argument("--table", required=True)
    parser.add_argument("--trigger_interval", required=True, default='40 seconds')
    parser.add_argument("--checkpoints", required=True, default='s3a://lake/spark/checkpoints/silver_writer/orders')

    args = parser.parse_args()
    spark = get_spark_streaming(app_name=f"Start ETL job [{args.table}]  from bornze to silve, Streaming",
                                 check_point_path = f"",
                                partitions=4)

    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    start_time = time.time()
    duration = args.duration
    print("0 - Prepere the spark to read schema")
    spark = initialize_read_strem(spark=spark)
    print("1 - Initialized successfully")   
    print("2 - Start reading ")

    q = (spark.writeStream
                .foreachBatch(partial(transform_then_load, 
                                     table_name=args.table,
                                     schema = orders_schema,
                                     ))
                .option("checkpointLocation", args.checkpoints)
                .outputMode("append")
                .trigger(processingTime = args.trigger_interval)
                .start() 
        ) 
    q.awaitTermination(timeout=args.duration)
    q.stop() 
    elapsed = time.time() - start_time
    print(f"Streaming stopped. Total elapsed: {elapsed:.0f}s")   