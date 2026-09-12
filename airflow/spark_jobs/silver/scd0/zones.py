import warnings
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import pyspark.sql.dataframe
from datetime import datetime
import pandas as pd
import sys

from tqdm import tqdm
import os
import argparse
from spark_jobs.shared.measure_running import run_func
#sys.path.append(os.path.abspath(".."))
#from utils.kafka_config import KafkaConfig
from spark_jobs.shared.spark_session import get_spark
import yaml
from pathlib import Path
import uuid
# Configure pandas to show ful output without truncation
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Don't truncate columns content
pd.set_option('display.width', None)        # Use full width

print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
from spark_jobs.ddl.tables_schema import  (
                                 customers_schema,
                                 restaurants_schema,
                                 zones_schema,
                                 cities_schema,
                                 
                                )

from spark_jobs.shared.watermark import get_last_watermark, update_watermark_table
from spark_jobs.shared.streaming_utils import fetch_bronze_layer_data ,flatten_kafka_payload
from spark_jobs.shared.update_state import write_state, read_state, get_latest_state_path

def extract_from_bronze(spark,
                                  table_name: str = 'zones'):
    """
    WHY separate extract:
    Reads bronze layer data using watermark to get only new/changed records
    since the last silver refresh. Isolated so failures here don't affect
    any transformation logic — safe to retry independently.
    """
    print(f"Extracting {table_name} from bronze layer...")
    
    last_watermark = get_last_watermark(
        spark=spark,
        table_name=table_name,
        layer='silver',
        prev_layer='bronze'
    )
    
    df_raw = fetch_bronze_layer_data(
        table_path_name=f"lake.bronze.{table_name}",
        water_mark=last_watermark,
        spark=spark
    )
    
    return df_raw, last_watermark


def transform(df_raw, schema, table_name: str = 'zones') -> pyspark.sql.DataFrame:
    """
    WHY separate transform:
    Flattens Kafka payload and deduplicates by LSN (last write wins per customer).
    Pure transformation — no I/O, no side effects.
    Safe to test independently with mock data.
    
    Returns None if no new data to process.
    """
    print(f"Transforming {table_name}...")
    
    df_flatern = flatten_kafka_payload(df = df_raw,
                                                       table_schema = schema )
        
    # remove the duplicates rows 
    window = Window.partitionBy("zone_id").orderBy(col("source_lsn").desc())        
    df_cdc = df_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
    
    
    print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())
    
    # df_changed.createOrReplaceTempView("restaurants_merge")
    print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())

    print(f"Transformed {df_cdc.count()} records for {table_name}")
    return df_cdc


def load_to_silver(df_transformed,last_watermark, spark, table_name: str = 'zones'):
    """
    WHY separate load:
    Executes the MERGE INTO silver table. Isolated so if the merge fails,
    we don't re-read bronze or re-transform — we just retry the merge.
    Watermark is only updated after a successful merge — guarantees
    no data is silently skipped on failure.
    """
    if df_transformed is None:
        print(f"No data to load for {table_name} — skipping")
        return
    
    print(f"Loading {table_name} to silver via MERGE...")
    
    df_transformed.createOrReplaceTempView("zones_updated")
        
    try:
        if  df_transformed.isEmpty():
            print("  No data in df_transformed to Upsert")
            return

        spark.sql("""
                        MERGE INTO lake.silver.zones z
                        USING zones_updated zu
                             on z.zone_id = zu.zone_id
                        WHEN MATCHED THEN UPDATE SET 
                                zone_id = zu.zone_id, 
                                city_id = zu.city_id,
                                zone_name = zu.zone_name,
                                is_active = zu.is_active,
                                created_at_ts = zu.created_at,
                                last_source_update_ts = zu.updated_at,
                                last_refresh_ts =  CURRENT_TIMESTAMP
                        WHEN NOT MATCHED THEN 
                            INSERT (
                                    zone_id,
                                    city_id,
                                    zone_name,
                                    is_active,
                                    created_at_ts,
                                    last_source_update_ts,
                                    last_refresh_ts
                                )
                                VALUES (
                                    zu.zone_id,
                                    zu.city_id,
                                    zu.zone_name,
                                    zu.is_active,
                                    zu.created_at,
                                    zu.updated_at,
                                    CURRENT_TIMESTAMP
                                )
                        """)

        
    
    except Exception as e:
        raise Exception(f"Error occred in function merge the {table_name.replace('_', ' ')} rows", e)
        
    
    # WHY watermark update is here and not in transform:
    # watermark must only advance after a successful write to silver.
    # If merge fails, watermark stays at previous value so next run
    # re-processes the same bronze records — no data loss.
    update_watermark_table(table_name=table_name)
    
    print(f"MERGE {table_name} bronze → silver: succeeded")
    print(f"  watermark advanced past: {last_watermark}")


def transforming_to_silver(type, 
                                     schema, 
                                     spark,
                                     current_state_path,
                                     prev_state_path,
                                     access_key,
                                     secret_key,
                                     table_name: str = 'zones'):
    """
    Orchestrator — calls extract → transform → load in sequence.
    
    WHY keep this wrapper:
    Existing callers don't need to change. The Airflow task or script
    that calls this function continues to work unchanged. The split
    is internal — each stage can also be called independently for
    debugging or partial reruns.
    """
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD0, CDC? No, transforming type: OVERWRITE")
    
    lw = "/last_watermark.json"
    preprocessed_cust_path = f"/{table_name}_raw_df.parquet"
    transformed_cust_path = f"/transformed_{table_name}.parquet"
    try:
        if type == 'extract':
            # extract
            df_raw, last_watermark = extract_from_bronze(spark=spark,
                                                                   table_name=table_name)
            # if  df_raw.isEmpty():
            #     raise ValueError(f"The data is empty, could not UPSERT, returned last_watermark {last_watermark}")
            # store the last_watermark
            write_state(key=current_state_path+lw, 
                        minio_access_key=access_key,
                        minio_secret_key=secret_key,
                        data= str(last_watermark)
                        )
            # store the preprocessed data
            write_state(key=current_state_path+preprocessed_cust_path, 
                        minio_access_key=access_key,
                        minio_secret_key=secret_key,
                        data=df_raw
                        )
        elif type == 'transform':
            # transform
            key_pre_cst = get_latest_state_path(table_name=table_name,
                                        minio_access_key=access_key,
                                        minio_secret_key=secret_key,
                                        prefix = "airflow/silver/pipeline-state/"
                                        ) or ''
            df_raw = read_state(
                key=key_pre_cst+preprocessed_cust_path, 
                minio_access_key=access_key,
                minio_secret_key=secret_key
            )
            df_transformed = transform(df_raw, schema, table_name)
            # store the transfomed state 
            write_state(key=current_state_path+transformed_cust_path, 
                        minio_access_key=access_key,
                        minio_secret_key=secret_key,
                        data=df_transformed
                        )
        elif type == 'load':    
            # load
            key_lw = get_latest_state_path(table_name=table_name,
                                        prefix= "airflow/silver/pipeline-state/",
                                        minio_access_key=access_key,
                                        minio_secret_key=secret_key,
                                        ) or ''
            # key_t = get_latest_state_path(table_name=table_name,
            #                             prefix= "airflow/silver/pipeline-state/",
            #                             minio_access_key=access_key,
            #                             minio_secret_key=secret_key,
            #                             ) or ''
            print("the latest state path:",key_lw)
            last_watermark = read_state(
                key=key_lw+lw,
                minio_access_key=access_key,
                minio_secret_key=secret_key
            )
            df_transformed = read_state(
                key=key_lw+transformed_cust_path,
                minio_access_key=access_key,
                minio_secret_key=secret_key
            )
            load_to_silver(df_transformed, 
                                     last_watermark,
                                     spark=spark,
                                     table_name=table_name)
        
    except Exception as e:
        raise Exception(e, f"transforming {table_name} bronze → silver: failed")
    
    
    
if '__main__' == __name__:
    spark = get_spark(app_name="Transform data from bornze to silve, Batches",partitions=1)
    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=False)
    parser.add_argument("--current_state_path", required=False, default="False")
    parser.add_argument("--prev_state_path", required=False, default="False")
    args = parser.parse_args()
    
    
    
    run_func(transforming_to_silver, args.type,
                                               zones_schema,
                                               spark,
                                               args.current_state_path,
                                               args.prev_state_path,
                                               access_key,
                                               secret_key,          
                                               "zones")