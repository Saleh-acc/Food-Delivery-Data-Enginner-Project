




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
print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
# to get the currnet path root for apps/ to call moudle there 

from spark_jobs.ddl.tables_schema import  (
                                 batch_tracker_schema
                                )
from spark_jobs.utils.postgres_config import PostgresConfig

















def insert_new_batches(tables, load_type = 'incremental', tolerate_with_duplicate = False):
    """ 
    Generate and insert new batch records into the bronze.batches_tracker table.

    This function scans the provided table metadata and creates tracking records
    for batches that have not been generated before. It prevents duplicate batch
    creation by checking existing records in the batch tracker.

    Workflow:
    1. Load existing batches from the tracker table.
    2. Check each new batch against previously generated batches.
    3. Skip batches that already exist (pending, failed, or optionally success).
    4. Create tracking records for new batches with a 'pending' status.
    5. Insert the new batch records into lake.bronze.batches_tracker.

    Args:
        tables (dict):
            Dictionary containing table configurations and generated batches.
            Expected format:
            {
                "customers": {
                    "new_batches": [
                        (1, 1, 1000),
                        (2, 1001, 2000)
                    ]
                }
            }

        load_type (str, optional):
            Type of load being processed (e.g., 'incremental', 'initial').
            This value is appended to the job name.
            Default is 'incremental'.

        tolerate_with_duplicate (bool, optional):
            If False (default), batches that were previously marked as
            'success' are skipped to avoid duplicate loading.

            If True, previously successful batches are ignored during the
            duplicate check, allowing the same data range to be generated
            again. This is mainly intended for full reloads or recovery
            scenarios where duplicate data insertion is acceptable.

    Returns:
        None
    """
    spark.catalog.clearCache()
    old_batches_q = "select * from lake.bronze.batches_tracker where status in ('pending', 'failed' {})"
    if tolerate_with_duplicate:
        old_batches_q = old_batches_q.format("")
    else:
        old_batches_q = old_batches_q.format(", 'success'")

    # ONE collect() to driver — plain Python from here on
    old_batches_rows = spark.sql(old_batches_q).collect()
    
    # build a set for fast lookup — no Spark, no executors, no Python workers
    existing = {
        (row.job_name, row.lower_bound, row.upper_bound)
        for row in old_batches_rows
    }

    data = []
    pbar = tqdm(tables, desc="Inserting New Batches", leave=True)
    for tp in pbar:
        job_name = tp + '_' + load_type
        pbar.set_postfix(table=tp, job_name=job_name)
        all_batches = tables.get(tp).get("new_batches")
        status = "pending"

        pbar2 = tqdm(all_batches, desc="all batches", leave=True)
        for batch_number, lower_bound, upper_bound in pbar2:
            pbar2.set_postfix(batch_number=batch_number,
                              lower_bound=lower_bound,
                              upper_bound=upper_bound)

            # plain Python set lookup — zero Spark involvement
            if (job_name, lower_bound, upper_bound) in existing:
                tqdm.write(
                    f"batch exists: {job_name}, "
                    f"batch_number {batch_number} — skipping"
                )
                print( f"batch exists: {job_name}, "
                    f"batch_number {batch_number} — skipping")
                continue

            data.append((
                str(uuid.uuid4()), job_name, batch_number,
                lower_bound, upper_bound, status,
                None, None, None, None, datetime.now()
            ))

    df_new_batches = spark.createDataFrame(data, batch_tracker_schema)
    df_new_batches.createOrReplaceTempView("new_batch_tracker")
    spark.sql("""
        INSERT INTO TABLE lake.bronze.batches_tracker
            SELECT * FROM new_batch_tracker;
    """)
    tqdm.write("inserting succeeded")



if __name__ == "__main__":
    spark = get_spark("Bronze - Insert batches")
    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="+", required=True)
    parser.add_argument("--current_state_path", required=True, default="False")
    parser.add_argument("--prev_state_path", required=True, default="False")
    parser.add_argument("--load_type", required=False, default="False")
    parser.add_argument("--tolerate_with_duplicate", required=False, default="False")
    args = parser.parse_args()
    
    print("passed args: ",args)
    latest_folder = get_latest_state_path(table_name='-'.join(args.config),
                                          minio_access_key= access_key ,
                                          minio_secret_key=secret_key
                                          )  or ''
    if latest_folder == '':
        raise Exception("This folder not found", latest_folder)
    tables_with_batches = read_state(key=latest_folder + args.prev_state_path,
                                    minio_access_key= access_key ,
                                     minio_secret_key=secret_key
                                     )
    print(latest_folder + args.prev_state_path)
    print(tables_with_batches)
    insert_new_batches(tables=tables_with_batches, 
                       load_type = args.load_type, 
                       tolerate_with_duplicate=args.tolerate_with_duplicate)
    # Write state to MinIO for next task
    write_state(key=args.current_state_path, 
                data= tables_with_batches,
                minio_access_key= access_key,
                minio_secret_key=secret_key
                )
    print(f"the metadata loaded on {args.current_state_path} ")