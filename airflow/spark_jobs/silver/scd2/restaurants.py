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

def extract_restaurants_from_bronze(spark,
                                  table_name: str = 'restaurants'):
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
    print(df_raw.show())
    print(last_watermark)
    return df_raw, last_watermark


def transform_restaurants(df_raw, schema, table_name: str = 'restaurants') -> pyspark.sql.DataFrame:
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
    window = Window.partitionBy("restaurant_id").orderBy(col("source_lsn").desc())        
    df_cdc = df_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
    
    df_silver_current = spark.sql("SELECT * FROM lake.silver.restaurants WHERE is_current = true")
    df_changed = df_cdc.join(
        df_silver_current,
        on="restaurant_id",
        how="left"
    ).filter(
        df_silver_current.restaurant_id.isNull() |
        (df_cdc.name != df_silver_current.name) |
        (df_cdc.cuisine_type != df_silver_current.cuisine_type) |
        (df_cdc.rating_avg != df_silver_current.rating_avg) |
        (df_cdc.is_active != df_silver_current.is_active) |
        (df_cdc.city_id != df_silver_current.city_id) |
        (df_cdc.zone_id != df_silver_current.zone_id) |
        (df_cdc.address != df_silver_current.address)
    ).select(df_cdc["*"])#.cache()
    
    # df_changed.createOrReplaceTempView("restaurants_merge")
    print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())
    print("  Is there data in the silver layer?",not df_silver_current.isEmpty())
    print("  Is there data after left join df_cdc with df_silver_current?",not df_changed.isEmpty())
    new_rows = df_changed.count()

    print(f"Transformed {new_rows} records for {table_name}")
    return df_changed


def load_customers_to_silver(df_transformed, last_watermark, spark, table_name: str = 'restaurants'):
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
    
    df_transformed.createOrReplaceTempView("restaurants_merge")
        
    try:
        if  df_transformed.isEmpty():
            print("  No data in df_transformed to Upsert")
            return
        spark.sql("""
                    MERGE INTO lake.silver.restaurants rest
                    USING restaurants_merge rest_updated
                            on rest.restaurant_id = rest_updated.restaurant_id
                            AND rest.is_current = True
                    WHEN MATCHED THEN UPDATE SET 
                                        rest.is_current = False,
                                        rest.eff_end_ts = CURRENT_TIMESTAMP,       
                                        rest.last_source_update_ts = rest_updated.updated_at
                    """)

        spark.sql("""
                    INSERT INTO lake.silver.restaurants SELECT 
                                    restaurant_id,
                                    name,
                                    cuisine_type,
                                    city_id,
                                    zone_id,
                                    address,
                                    rating_avg,
                                    is_active,
                                    True,
                                    CURRENT_TIMESTAMP,
                                    NULL,
                                    onboarded_at,
                                    created_at as created_at_ts,
                                    updated_at as last_source_update_ts,
                                    CURRENT_TIMESTAMP  
                                FROM restaurants_merge rest_updated              
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


def transforming_restaurants_to_silver(type, 
                                     schema, 
                                     spark,
                                     current_state_path,
                                     prev_state_path,
                                     access_key,
                                     secret_key,
                                     table_name: str = 'customers'):
    """
    Orchestrator — calls extract → transform → load in sequence.
    
    WHY keep this wrapper:
    Existing callers don't need to change. The Airflow task or script
    that calls this function continues to work unchanged. The split
    is internal — each stage can also be called independently for
    debugging or partial reruns.
    """
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD2, CDC? No, transforming type: MERGE")
    
    lw = "/last_watermark"
    preprocessed_cust_path = "/restaurants_raw_df.parquet"
    transformed_cust_path = "/transformed_restaurants.parquet"
    try:
        if type == 'extract':
            # extract
            df_raw, last_watermark = extract_restaurants_from_bronze(spark=spark,
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
                key=key_pre_cst+'restaurants_raw_df.parquet', 
                minio_access_key=access_key,
                minio_secret_key=secret_key
            )
            df_transformed = transform_restaurants(df_raw, schema, table_name)
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
            print(key_lw)
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
            load_customers_to_silver(df_transformed, 
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
    
    
    
    run_func(transforming_restaurants_to_silver, args.type,
                                               restaurants_schema,
                                               spark,
                                               args.current_state_path,
                                               args.prev_state_path,
                                               access_key,
                                               secret_key,          
                                               "restaurants")