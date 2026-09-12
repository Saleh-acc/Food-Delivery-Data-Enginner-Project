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
                                        payments_schema        
                                )
from spark_jobs.shared.watermark import  update_watermark_table
from spark_jobs.shared.streaming_utils import flatten_kafka_payload

def initialize_read_strem(spark:pyspark.sql.session.SparkSession):
    return (
        spark.readStream
        .format("iceberg")
        #.option("stream-from-timestamp", "0")   #  start from the beginning
        .option("streaming-max-files-per-micro-batch", "50")

        .load("lake.bronze.payments")
    )

def transform(df_raw: pyspark.sql.DataFrame, 
              batch_id:int, 
              schema:pyspark.sql.types.StructType, 
              table_name: str = 'payments') -> pyspark.sql.DataFrame:
    """
    WHY separate transform:
    Flattens Kafka payload and deduplicates by LSN (last write wins per customer).
    Pure transformation — no I/O, no side effects.
    Safe to test independently with mock data.
    
    Returns None if no new data to process.
    """
    print(f"[{table_name}] Transforming, table type: FACT, CDC? Yes, transforming type: MERGE")
    
    df_flatern = flatten_kafka_payload(df = df_raw,
                                                       table_schema = schema )
        
    # remove the duplicates rows 
    window = Window.partitionBy("payment_id").orderBy(col("source_lsn").desc())

    # remove duplicate rows 
    df_flattened_partitioned = df_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
    max_lsn = df_flattened_partitioned.agg({"source_lsn": "max"}).collect()[0][0] if "source_lsn" in df_flattened_partitioned.columns else None
    
    print(f"[{table_name}] batch {batch_id}:"
              f"max_source_lsn={max_lsn}")
    df_flattened_partitioned = df_flattened_partitioned.select(
                            "payment_id",
                            "order_id",
                            "payment_method",
                            "amount",
                            "status",
                            "processor_ref" ,
                            "paid_at",
                            "created_at",
                            "updated_at"     
                )
    df_flattened_partitioned = df_flattened_partitioned.withColumnRenamed("updated_at", "last_source_update_ts")
    df_flattened_partitioned = df_flattened_partitioned.withColumn("last_refresh_ts", current_timestamp())

    #print(f"Transformed {df_flattened_partitioned.count()} records for {table_name}")
    return df_flattened_partitioned


def load_to_silver(df_transformed:pyspark.sql.DataFrame,
                   table_name: str = 'payments'):
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
    df_transformed.createOrReplaceTempView("silver_payments_updates")
        # Compute a REAL progress marker before the MERGE
    
    
    try:

       
        source = spark.table("silver_payments_updates").alias("s")
        target = spark.table("lake.silver.payments").alias("t")
    
        updates = (
            source
            .join(target, on="payment_id", how="left")
            .withColumn(
                "action",
                when(col("t.payment_id").isNotNull(), "update")
                .otherwise("insert")
            )
        )
    
        print(f"[{table_name}]  Is there data after left join the bronze layer with silver layer?",not updates.isEmpty())
        print(f"[{table_name}] total of processed rows:")
        # if the load is havy use .cache() then .persist to release  the memory 
        updates.groupBy("action").count().show()
        
        spark.sql("""
            MERGE INTO lake.silver.payments od
            USING silver_payments_updates s
            ON od.payment_id = s.payment_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        print(f"[{table_name}] MERGE. data from bronze --> silver: successed")

        
        
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
    parser.add_argument("--checkpoints", required=True, default='s3a://lake/spark/checkpoints/silver_writer/payments')

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
                                     schema = payments_schema,
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