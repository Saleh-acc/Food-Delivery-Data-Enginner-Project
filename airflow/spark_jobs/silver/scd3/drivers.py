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
                                        drivers_schema        
                                )
from spark_jobs.shared.watermark import  update_watermark_table
from spark_jobs.shared.streaming_utils import flatten_kafka_payload

def initialize_read_strem(spark:pyspark.sql.session.SparkSession):
    return (
        spark.readStream
        .format("iceberg")
        #.option("stream-from-timestamp", "0")   #  start from the beginning
        .option("streaming-max-files-per-micro-batch", "50")

        .load("lake.bronze.drivers")
    )

def transform(df_raw: pyspark.sql.DataFrame, 
              batch_id:int, 
              schema:pyspark.sql.types.StructType, 
              table_name: str = 'drivers') -> pyspark.sql.DataFrame:
    """
    WHY separate transform:
    Flattens Kafka payload and deduplicates by LSN (last write wins per customer).
    Pure transformation — no I/O, no side effects.
    Safe to test independently with mock data.
    
    Returns None if no new data to process.
    """
    print(f"[{table_name}] Transforming, table type: DIM: SCD3, CDC? Yes, transforming type: MERGE")
    
    df_drivers_flatern = flatten_kafka_payload(df = df_raw,
                                                          table_schema = schema )
   
    max_lsn = df_drivers_flatern.agg({"source_lsn": "max"}).collect()[0][0] if "source_lsn" in df_drivers_flatern.columns else None
        
    print(f"[{table_name}] batch {batch_id}:"
                  f"max_source_lsn={max_lsn}")

     # remove the duplicates rows 
    window = Window.partitionBy("driver_id").orderBy(col("source_lsn").desc())        
    df_cdc_after = df_drivers_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
    
    print("Is there Captured data from kafka?",not df_cdc_after.isEmpty())

    return df_cdc_after


def load_to_silver(df_transformed:pyspark.sql.DataFrame,
                   table_name: str = 'drivers'):
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
    df_transformed.createOrReplaceTempView("drivers_updates")    
    
    print(f"[{table_name}] total of processed rows {df_transformed.count()}:")
    
    try:
                  
        spark.sql("""
                                  MERGE INTO lake.silver.drivers dr
                                  USING drivers_updates dru
                                      ON dr.driver_id = dru.driver_id
                                  WHEN MATCHED AND (
                                                          dr.city_id != dru.city_id OR
                                                          dr.vehicle_type != dru.vehicle_type OR
                                                          dr.status != dru.status OR
                                                          dr.phone != dru.phone OR
                                                          dr.is_active != dru.is_active
                                                      ) THEN
                                      UPDATE SET
                                         full_name = dru.full_name,
                                         phone = dru.phone,
                                         vehicle_type = dru.vehicle_type,
                                         city_id = dru.city_id,
                                         status = dru.status,
                                         is_active = dru.is_active,
                                         onboarded_at_ts = dru.onboarded_at,
                                         created_at_ts =  dru.created_at,
                                         prev_vehicle_type = CASE WHEN dr.vehicle_type != dru.vehicle_type THEN dr.vehicle_type ELSE dr.prev_vehicle_type END ,
                                         prev_city_id = CASE WHEN dr.city_id != dru.city_id THEN dr.city_id ELSE dr.prev_city_id END,
                                         last_source_update_ts = dru.updated_at,
                                         last_refresh_ts = CURRENT_TIMESTAMP
                                 WHEN NOT MATCHED THEN
                                     INSERT (
                                              driver_id,
                                              full_name,
                                              phone,
                                              vehicle_type,
                                              city_id,
                                              status,
                                              is_active,
                                              onboarded_at_ts,
                                              created_at_ts,
                                              prev_vehicle_type,
                                              prev_city_id,
                                              last_refresh_ts,
                                              last_source_update_ts
                                          )
                                          VALUES (
                                              dru.driver_id,
                                              dru.full_name,
                                              dru.phone,
                                              dru.vehicle_type,
                                              dru.city_id,
                                              dru.status,
                                              dru.is_active,
                                              dru.onboarded_at,
                                              dru.created_at,
                                              NULL,
                                              NULL,
                                              CURRENT_TIMESTAMP,
                                              dru.updated_at
                                          )
                                                                  
                      """)
        # if the load is havy use .cache() then .persist to release  the memory 
        
        print(f"[{table_name}] MERGE. data from bronze --> silver: successed")

        
        
        # check remaining time after each batch
        elapsed = time.time() - start_time
        remaining = args.duration - elapsed
        print(f"[{table_name}] Remaining: {remaining}")
    except Exception as e:
        raise Exception(f"[{table_name}] Could not UPSERT the data for table {table_name.replace('_', ' ')} rows", e)
        
    
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
    parser.add_argument("--checkpoints", required=True, default='s3a://lake/spark/checkpoints/silver_writer/drivers')

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
                                     schema = drivers_schema,
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