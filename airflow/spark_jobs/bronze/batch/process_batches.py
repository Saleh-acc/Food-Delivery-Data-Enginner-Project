




import warnings
import pyspark.sql.functions as F
from pyspark.sql.types import *
# import pyspark.sql.dataframe
from datetime import datetime
from copy import deepcopy
import pandas as pd
import sys
from tqdm import tqdm
import os
from pyspark.sql.utils import AnalysisException
import json
#sys.path.append(os.path.abspath(".."))
#from utils.kafka_config import KafkaConfig
import yaml
from pathlib import Path
import uuid
import argparse
from spark_jobs.shared.spark_session import get_spark
from spark_jobs.shared.update_state import write_state, read_state, get_latest_state_path
from spark_jobs.shared.read_postgres_table import  read_postgres_table_parallel

print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
# to get the currnet path root for apps/ to call moudle there 

from spark_jobs.ddl.tables_schema import  (
                                 batch_tracker_schema
                                )
from spark_jobs.utils.postgres_config import PostgresConfig


    
POSTGRES_CONFIG = PostgresConfig(config_path =  "/opt/airflow/config/batches_mapping.yaml")._get_config_json()
jdbc_url = POSTGRES_CONFIG.get("postgress").get("jdbc_url")
jdbc_properties = POSTGRES_CONFIG.get("postgress").get("jdbc_properties")
jdbc_properties =  {k: v for d in jdbc_properties for k, v in d.items()}
bucket_name = POSTGRES_CONFIG.get("bucket").get("bronze-layer")




spark = None

def mark_running(row_id:str):
    """
    Mark a batch as running in the batch tracker table.

    This function updates the status of a specific batch to `running`
    and records the batch start timestamp. After the update, it performs
    a validation step to ensure that the status change was applied
    successfully.

    Workflow:
    1. Update the batch status to `running`.
    2. Set `started_at_ts` to the current timestamp.
    3. Read the updated record from the tracker table.
    4. Verify that the batch exists.
    5. Verify that the status was correctly updated.
    6. Log the operation result.

    Args:
        row_id (str):
            Unique identifier of the batch record in
            `lake.bronze.batches_tracker`.

    Returns:
        None

    Raises:
        ValueError:
            - If the batch record does not exist.
            - If the status update was not applied successfully.

        AnalysisException:
            If the tracker table is missing or the SQL statement
            cannot be executed.

        Exception:
            Re-raises any unexpected errors after logging them.
    """
    try:
        spark.sql(f"""
            UPDATE lake.bronze.batches_tracker SET
                status = 'running',
                started_at_ts = CURRENT_TIMESTAMP
            WHERE id = '{row_id}'
        """)
        check = spark.sql(f"""
                SELECT status FROM lake.bronze.batches_tracker
                WHERE id = '{row_id}'
            """).collect()
        
        if not check:
            raise ValueError(f" Batch {row_id} not found in tracker")
        if check[0]["status"] != "running":
            raise ValueError(f"Failed to update batch {row_id} status")
        tqdm.write(f" status change to running. Successed for batch {row_id}")
    except AnalysisException as e:
        # Table missing or bad SQL — fatal
        tqdm.write(f" Fatal in mark_running func: tracker table issue — {e}")
        raise
    except Exception as e:
        # Connection or other — log and raise
        tqdm.write(f" Failed in mark_running func to mark batch {row_id} as running — {e}")
        raise

def mark_success(row_id:str, rows_processed:int):
    """
    Mark a batch as successfully completed in the batch tracker table.

    This function updates a batch record in `lake.bronze.batches_tracker`
    by setting its status to `success`, recording the completion timestamp,
    and storing the number of processed rows. It also validates that the
    update was applied correctly.

    Workflow:
    1. Update batch status to `success`.
    2. Set `completed_at_ts` to the current timestamp.
    3. Store the number of processed rows.
    4. Re-read the record from the tracker table.
    5. Validate existence of the batch.
    6. Confirm the status update was applied successfully.
    7. Log the successful completion.

    Args:
        row_id (str):
            Unique identifier of the batch record in
            `lake.bronze.batches_tracker`.

        rows_processed (int):
            Number of rows successfully processed in this batch.

    Returns:
        None

    Raises:
        ValueError:
            - If the batch record does not exist in the tracker table.
            - If the status update fails or is not reflected correctly.

        AnalysisException:
            If the tracker table is missing or the SQL update fails.

        Exception:
            Any unexpected error during execution is logged and re-raised.

    """
    try:
        spark.sql(f""" 
               UPDATE lake.bronze.batches_tracker SET
                   status = 'success', 
                   completed_at_ts = CURRENT_TIMESTAMP,
                   rows_processed = {rows_processed}   
               WHERE id = '{row_id}'
           """)
        check = spark.sql(f"""
                SELECT status FROM lake.bronze.batches_tracker
                WHERE id = '{row_id}'
            """).collect()

        if not check:
            raise ValueError(f"Batch {row_id} not found in tracker")
        if check[0]["status"] != "success":
            raise ValueError(f" Failed to update batch {row_id} status")
        tqdm.write(f" status change to success. Successed for batch {row_id} and row processed {rows_processed}")
    except AnalysisException as e:
       
        tqdm.write(f" Fatal in mark_success func: tracker table issue — {e} ")
        raise
    except Exception as e:
        tqdm.write(f" Failed in mark_success func to mark batch {row_id} as running — {e}")
        raise

def mark_failed(row_id:str, batch_number:str, error_msg:str):
    """
    Mark a batch as failed in the batch tracker table.

    This function updates the status of a batch in
    `lake.bronze.batches_tracker` to `failed`. It is typically used when
    a batch execution encounters an error. After updating the status,
    it validates that the update was successfully applied.

    Workflow:
    1. Update the batch status to `failed`.
    2. (Optionally) store or associate the failure reason.
    3. Re-read the batch record from the tracker table.
    4. Validate that the batch exists.
    5. Confirm that the status has been updated to `failed`.
    6. Log the failure event.

    Args:
        row_id (str):
            Unique identifier of the batch record in
            `lake.bronze.batches_tracker`.

        batch_number (str):
            Logical batch number used for tracking and logging purposes.

        error_msg (str):
            Error message describing the reason for the failure.

    Returns:
        None

    Raises:
        ValueError:
            - If the batch record does not exist in the tracker table.
            - If the status update is not reflected correctly.

        AnalysisException:
            If the tracker table is missing or the SQL statement fails.

        Exception:
            Any unexpected error is logged and re-raised.

    """
    try:
        spark.sql(f""" 
               UPDATE lake.bronze.batches_tracker SET
                   status = 'failed',
                   error_message = '{error_msg}'
                   
               WHERE id = '{row_id}'
                   
           """)
        check = spark.sql(f"""
                SELECT status FROM lake.bronze.batches_tracker
                WHERE id = '{row_id}'
            """).collect()

        if not check:
            raise ValueError(f"Batch {row_id} not found in tracker")
        if check[0]["status"] != "failed":
            raise ValueError(f" Failed to update batch {row_id} status")
        tqdm.write( f"Status changed to 'failed' for batch {row_id}, batch number: {batch_number}")
    except AnalysisException as e:
       
        tqdm.write(f" Fatal in mark_failed func: tracker table issue — {e} ")
        raise
    except Exception as e:
        tqdm.write(f" Failed in mark_failed func to mark batch {row_id} as running — {e}")
        raise
    
   


def process_batches(tables:dict):
    """
    Execute batch processing for all tables using the batch tracker system.

    This function is the main orchestration layer for processing data
    batches in a Spark-based ingestion pipeline. It reads pending or
    failed batches from the batch tracker, extracts data from source
    tables using the defined batch ranges, transforms the data into a
    standardized ingestion format, and writes it to the target storage (bronze layer).

    Workflow:
    1. Iterate over all tables defined in the configuration.
    2. Fetch batches from `lake.bronze.batches_tracker` that are in
       `pending` or `failed` state.
    3. Skip tables that have no pending or failed batches.
    4. For each batch:
        a. Mark batch as `running`.
        b. Read data from the source table using batch bounds
           (lower_bound, upper_bound).
        c. Load data in parallel using partitioned reads.
        d. Add ingestion metadata columns (Kafka-style structure).
        e. Write transformed data to the target storage path.
        f. Mark batch as `success` with processed row count.
    5. If any error occurs:
        - Mark batch as `failed`.
        - Log the error and continue processing.

    Args:
        tables (dict):
            Dictionary containing table metadata and batch definitions.

            Example:
            {
                "customers": {
                    "PK": "customer_id",
                    "count": 41800,
                    "min_id": 1,
                    "max_id": 41800,
                    "new_batches": [
                        (0, 1, 41800)
                    ]
                }
            }

    Returns:
        None


    Raises:
        None (all exceptions are handled per batch).
    """
    pbar =  tqdm(list(tables.keys()), desc="Processing all tables", leave=True)
   # pbar.set_description(f"Processing all tables")
    for table in pbar:
        pbar.set_postfix(table=table)
        batch = spark.sql(f"""SELECT * 
                            FROM lake.bronze.batches_tracker 
                            WHERE job_name like '{table}%' and status in ('pending', 'failed') 
                            ORDER BY batch_number  """)
        if batch.isEmpty():
            tqdm.write(f"There is no batch in status pending or failed for table: {table}:")
           
            continue 
    
        batch.createOrReplaceTempView("updated_batches_tracker")
        
        #pbar2 = tqdm(batch.collect())
        for row in batch.collect():
           table_PK = tables.get(table).get("PK")
           path = f"{bucket_name}.{table}"
           tqdm.write(f" Start processing for table {table}: \n batch number:{row.batch_number}\n batch id: {row.id}\n lower bound: {row.lower_bound}\n upper bound: {row.upper_bound}\n crrent status: {row.status} ")
           #tqdm.write(row.id, row.job_name, row.batch_number, row.lower_bound, row.upper_bound, row.status) 

           try:
               mark_running(row.id)
                
              
               general_q = f""" 
                          ( SELECT * 
                          FROM {table} as t 
                          WHERE {table_PK} >= '{row.lower_bound}'
                                  AND {table_PK} <= '{row.upper_bound}'
                          ) as q
                          """
               #tqdm.write(general_q)
               df = read_postgres_table_parallel(sql_query = general_q,
                                          partition_column= table_PK, 
                                          lower = row.lower_bound, 
                                          upper = row.upper_bound, 
                                          num_partitions=4,
                                          jdbc_properties=jdbc_properties,
                                          jdbc_url=jdbc_url
                                          )
               
               added_kafka_columns = df.select(
                                    F.lit("inital_load_"+table).alias("kafka_topic"),
                                    F.lit(None).alias("kafka_partition"),
                                    F.lit(None).alias("kafka_offset"),
                                    F.lit(None).alias("kafka_timestamp"),                 
                                    F.lit('r').alias("operation"),
                                    F.lit(None).alias("before_payload"),
                                    F.expr(f"to_json(struct({', '.join(df.columns)}))").alias("after_payload"),             
                                    F.lit(None).alias("source_ts_ms"),
                                    F.lit(1).alias("source_lsn"),
                                    F.lit(None).alias("source_txid"),
                                    F.lit(table).alias("source_table"),
                                    F.lit(table).alias("source_snapshot"),
                                    F.current_timestamp().alias("ingestion_ts")
                            )
               #tqdm.write_as_df(added_kafka_columns)
               rows_processed = df.count()
               tqdm.write(f" Number of rows processed {rows_processed}")
               tqdm.write(f" Path that will be weriting in: {path} ")
               (
                added_kafka_columns
                    .writeTo(path)
                    .append()
                )
               mark_success(row.id, rows_processed)
               tqdm.write("="*50)
           except Exception as e:
                mark_failed(row.id, row.batch_number, e)
                raise Exception(e)
                tqdm.write(e)
   
   
   
   
   
   
   
   
if __name__ == "__main__":
    spark = get_spark("Bronze - process batches")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    spark.sparkContext.setLogLevel("WARN")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="+", required=True)
    parser.add_argument("--prev_state_path", required=True, default="False")
    args = parser.parse_args()
    
    print("passed args: ",args)
    latest_folder = get_latest_state_path(table_name='-'.join(args.config),
                                            minio_access_key= access_key ,
                                            minio_secret_key=secret_key
                                          ) or ''
    if latest_folder == '':
        raise Exception("This folder not found", latest_folder)
    tables_with_batches = read_state(key=latest_folder + args.prev_state_path,
                                    minio_access_key= access_key ,
                                    minio_secret_key=secret_key
                                     )
    print(latest_folder + args.prev_state_path)
    print(tables_with_batches)
    
    process_batches(tables=tables_with_batches)
    
    print(f"the metadata loaded on {args.prev_state_path} ")