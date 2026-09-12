

import warnings
import pyspark.sql.functions as F
from pyspark.sql.types import *
# import pyspark.sql.dataframe

from tqdm import tqdm

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


def generate_batches(tables, batch_size =100_000):
    """
    Generate ID-based batches for each table.

    This function divides the records of each table into smaller chunks
    based on the specified batch size. The generated batches are stored
    in the `new_batches` key of each table configuration.

    Each batch is represented as a tuple:

        (batch_number, lower_bound, upper_bound)

    Workflow:
    1. Iterate through all tables.
    2. Determine the effective batch size for each table.
    3. Split the ID range into multiple batches.
    4. Store the generated batches inside `tables[table]["new_batches"]`.
    5. Return the updated tables dictionary.

    Args:
        tables (dict):
            Dictionary containing table metadata.
            Expected format:
            {
                "customers": {
                    "count": 1000000,
                    "min_id": 1
                }
            }

        batch_size (int, optional):
            Maximum number of records per batch.
            Default is 100,000.

    Returns:
        dict:
            The updated tables dictionary with a `new_batches`
            key added to each table.

            Example:
            {
                "customers": {
                    "count": 250000,
                    "min_id": 1,
                    "new_batches": [
                        (0, 1, 100000),
                        (1, 100001, 200000),
                        (2, 200001, 250000)
                    ]
                }
            }

    Note:
        - If the total record count is smaller than the batch size,
          a single batch is generated.
        - The last batch may contain fewer records than the specified
          batch size.
    """
    pbar = tqdm(tables, desc="Generate Batches", leave=True)
    for t in pbar:
        pbar.set_postfix(table = t)
        tables[t]["new_batches"] = list()
        count = tables.get(t).get("count")
        batch_size_per_table = min(batch_size, count)
        min_id = tables.get(t).get("min_id")
        tqdm.write(f"{t}: batch_size_per_table {batch_size_per_table}, count: {count}")
        for idx, a in enumerate(range(min_id, count, batch_size_per_table)):
            
            if  count > batch_size_per_table+a-1:
                tp = (idx, a, batch_size_per_table+a-1)
         
                tables[t]["new_batches"].append(tp)
            else:
                tp = (idx, a, count)
            
                tables[t]["new_batches"].append(tp)
                        
    print(tables)
    return tables


if __name__ == "__main__":
    spark = get_spark("Bronze - Generate batches")
    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="+", required=True)
    parser.add_argument("--current_state_path", required=True, default="False")
    parser.add_argument("--prev_state_path", required=True, default="False")
    args = parser.parse_args()
    
    print("args passed: ",args)
    print(type(args.config))
    latest_folder = get_latest_state_path(table_name='-'.join(args.config),
                                          minio_access_key= access_key ,
                                          minio_secret_key=secret_key
                                          )  or ''
    if latest_folder == '':
        raise Exception("This folder not found", latest_folder)
    print(latest_folder + args.prev_state_path)
    tables_with_details = read_state(key=latest_folder + args.prev_state_path,
                                     minio_access_key= access_key ,
                                     minio_secret_key=secret_key
                                     )

    print("tables_with_details: ",tables_with_details)
    tables_with_batches = generate_batches(tables_with_details) or {}
    # Write state to MinIO for next task
    print("current state path: ",args.current_state_path)

    write_state(key=args.current_state_path, 
                data= tables_with_batches,
                minio_access_key= access_key ,
                minio_secret_key=secret_key
                )
    print(f"the metadata loaded on {args.current_state_path} ")