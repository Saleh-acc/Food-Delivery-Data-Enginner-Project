import warnings
import pyspark.sql.functions as F
from pyspark.sql.types import *
# import pyspark.sql.dataframe
from copy import deepcopy
import sys
from tqdm import tqdm
from pathlib import Path

import os
import json
#sys.path.append(os.path.abspath(".."))
#from utils.kafka_config import KafkaConfig
import argparse
from spark_jobs.shared.spark_session import get_spark
print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
from spark_jobs.shared.read_postgres_table import read_postgres_table
from spark_jobs.utils.postgres_config import PostgresConfig
from spark_jobs.shared.load_tables_config import load_tables_config
from spark_jobs.shared.update_state import write_state 
from datetime import datetime

POSTGRES_CONFIG = PostgresConfig(config_path =  "/opt/airflow/config/batches_mapping.yaml")._get_config_json()
jdbc_url = POSTGRES_CONFIG.get("postgress").get("jdbc_url")
jdbc_properties = POSTGRES_CONFIG.get("postgress").get("jdbc_properties")
jdbc_properties =  {k: v for d in jdbc_properties for k, v in d.items()}
bucket_name = POSTGRES_CONFIG.get("bucket").get("bronze-layer")




def get_metadata_tables(tables:dict, 
                        jdbc_url,
                        jdbc_properties,
                        initial_load = False, 
                        is_initial_load_with_cdc = False,
                        ):
    """
    Retrieve metadata required for batch generation from the source tables.

    This function collects the total number of records and the ID boundaries
    (minimum and maximum primary key values) for each table. The collected
    metadata is later used to generate extraction batches.

    Depending on the loading strategy, the function supports both initial
    and incremental loads:

    - Initial Load:
        Retrieves metadata for the entire table.

    - Incremental Load:
        Retrieves metadata only for records updated after the last generated
        batch timestamp stored in the batch tracker table.

    The function also handles CDC (Change Data Capture) tables according to
    the provided configuration flags.

    Workflow:
    1. Create a copy of the input tables dictionary.
    2. Optionally retrieve the last generated batch timestamps.
    3. Iterate through all configured tables.
    4. Skip or include CDC tables based on the load configuration.
    5. Build the appropriate metadata query.
    6. Retrieve count, maximum ID, and minimum ID from the source.
    7. Remove tables with no new records.
    8. Store the collected metadata inside the table configuration.
    9. Return the updated tables dictionary.

    Args:
        tables (dict):
            Dictionary containing table configurations.
            Example:
            {
                "customers": {
                    "PK": "customer_id",
                    "is_cdc": False
                }
            }

        initial_load (bool, optional):
            Determines the loading strategy.

            - True: Read metadata for the entire source table.
            - False: Read metadata only for newly updated records.

            Default is False.

        is_initial_load_with_cdc (bool, optional):
            Controls whether CDC tables should participate in the initial load.

            - True: Include CDC tables during the initial load.
            - False: Exclude CDC tables.

            Default is False.

    Returns:
        dict:
            Updated tables dictionary containing:

            - count
            - max_id
            - min_id

            Example:
            {
                "customers": {
                    "PK": "customer_id",
                    "is_cdc": False,
                    "count": 500000,
                    "max_id": 500000,
                    "min_id": 1
                }
            }

    Raises:
        ValueError:
            If an incremental load is requested but no previous successful
            batch generation exists in the batch tracker table.

    Note:
        - Incremental loading assumes that source tables contain an
          `updated_at` column.
        - CDC tables are automatically excluded unless explicitly enabled
          for the initial load.
        - Tables with no new or updated records are removed from the
          returned dictionary.
    """
    tables = deepcopy(tables)
    last_generated_batches = spark.createDataFrame([], "job_name STRING, generated_at_ts TIMESTAMP")
    def get_metadata_query(t, table_pk, initial_load, last_generated_batches = spark.createDataFrame([], "job_name STRING, generated_at_ts TIMESTAMP")):
        
        if initial_load:
            tqdm.write("technique type: inital load")

            return  f"""
                    SELECT count(*), max({table_pk}), min({table_pk})  
                    FROM {t} 
                    """
        else:
            tqdm.write("technique type: incremental load")
            
            last_generated_batch = (
                                        last_generated_batches
                                        .filter(F.col("job_name").contains("customers"))
                                        .agg(F.max("generated_at_ts").alias("generated_at_ts"))
                                    ).collect()[0][0]
            if not last_generated_batch:
                raise ValueError(f"The last_generated_batch value is {last_generated_batch} which is means the batch tracker table mostly is empty no inital load occrs before so that can't start incremental load technique, you must start inital load first.")
            tqdm.write(f"Last generated batch timestamp: {last_generated_batch}")
            return  f"""
                    SELECT count(*), max({table_pk}), min({table_pk})  
                    FROM {t} 
                    WHERE updated_at >= '{last_generated_batch}'
                    """
    pbar = tqdm(tables)
    # is_cdc:false and is_initial_load_with_cdc:true --> no effect 
    # is_cdc:false and is_initial_load_with_cdc:false --> no effect 
    # is_cdc:true and initial_load:true and is_initial_load_with_cdc:false --> just inital load without cdc tables
    # is_cdc:true and initial_load:false and is_initial_load_with_cdc:true --> inital load for all tables
    pbar.set_description(f"Get Metadata Tables")
    for idx, t in enumerate(list(pbar)):
        if tables.get(t).get("is_cdc"):
            # skip the cdc tables type which usualy dose not have update_date_ts column and drop it from the object
            if not (is_initial_load_with_cdc and initial_load):
                del tables[t]
                tqdm.write(f"table {t} is cdc, reomved from the dict")
                continue 
        if not initial_load:
            last_generated_batches = spark.sql(f""" 
                                        SELECT job_name, max(generated_at_ts) as generated_at_ts
                                        FROM lake.bronze.batches_tracker
                                        GROUP BY job_name
                        """).cache()
        
        table_pk = tables.get(t).get('PK')
        
        table_metadata_q = get_metadata_query(t = t, 
                                              table_pk = table_pk, 
                                              initial_load = initial_load, last_generated_batches= last_generated_batches)
        print(f"table name:{t}\n table PK:{table_pk}")
        df = read_postgres_table(table_metadata_q,
                                 jdbc_url= jdbc_url,
                                 jdbc_properties=jdbc_properties,
                                 spark_session=spark
                                 )

        row = df.collect()[0]
        count = row[0]
        max_id = row[1]
        min_id = row[2]

        if not count:
            tqdm.write(f" No update occur in source. so will be skiped and removed from dict\n")
            del tables[t] 
            continue
        tables[t]['max_id'] = max_id
        tables[t]['min_id'] = min_id
        tables[t]['count'] = count
        tqdm.write(f"{idx}, table:{t}, details:{tables.get(t)}\n") 
        tqdm.write("==================================================================================")
    if last_generated_batches:
        last_generated_batches.unpersist()
    return tables


if __name__ == "__main__":
    spark = get_spark("Bronze - Get Metadata")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    print(access_key)
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", nargs="+", required=True)
    parser.add_argument("--load_type", default="inital_load")
    parser.add_argument("--is_initial_load_with_cdc", default="False")
    parser.add_argument("--current_state_path", required=True)
    args = parser.parse_args()

    tables = load_tables_config(args.config, Path(__file__).parent / "tables.json")
    print(tables)
    tables_with_metadata = get_metadata_tables(
                            tables, 
                            jdbc_url,
                            jdbc_properties,
                            initial_load = (args.load_type == 'inital_load'), 
                            is_initial_load_with_cdc = args.is_initial_load_with_cdc,
    )

    # Write state to MinIO for next task
    write_state(key=args.current_state_path, 
                data=tables_with_metadata,
                minio_access_key= access_key ,
                minio_secret_key=secret_key
                )
    print(f"the metadata loaded on {args.current_state_path} ")