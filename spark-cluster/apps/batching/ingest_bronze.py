import warnings
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.sql.dataframe
from datetime import datetime
from copy import deepcopy
from IPython.display import display
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
# Configure pandas to show ful output without truncation
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.max_colwidth', None) # Don't truncate columns content
pd.set_option('display.width', None)        # Use full width

print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
# to get the currnet path root for apps/ to call moudle there 
sys.path.append(os.path.abspath("/opt/spark-apps"))
print("="*50)
print(os.path.abspath("/opt/spark-apps/"))
print("="*50)
from ddl.tables_schema import  (
                                 batch_tracker_schema
                                )
from utils.postgres_config import PostgresConfig

POSTGRES_CONFIG = PostgresConfig()._get_config_json()
jdbc_url = POSTGRES_CONFIG.get("postgress").get("jdbc_url")
jdbc_properties = POSTGRES_CONFIG.get("postgress").get("jdbc_properties")
jdbc_properties =  {k: v for d in jdbc_properties for k, v in d.items()}
bucket_name = POSTGRES_CONFIG.get("bucket").get("bronze-layer")

spark = (
    SparkSession
    .builder
    .appName("Ingest data from source to bronze layer, Batching")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config("spark.sql.shuffle.partitions", 4)
    # .config(
    #     "spark.jars.packages",
    #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    # )
    .config(
        "spark.sql.debug.maxToStringFields", "1000"
    )
    .config(
        "spark.sql.adaptive.enabled", "true"
    )
    .config(
        "spark.sql.debug.maxToStringFields", "1000"
    )
    # .config(
    #   "spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION
    # )
#    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(" Libraries imported successfully")
# declare the constant as template 
tables = {
    "customers":{
        "PK":"customer_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":False
    },
    "cities":{
        "PK":"city_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":False
    },
    "zones":{
        "PK":"zone_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":False
    },
    "restaurants":{
        "PK":"restaurant_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":False
    },
    "orders":{
        "PK":"order_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "order_items":{
        "PK":"order_item_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "payments":{
        "PK":"payment_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "order_status_events":{
        "PK":"event_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "reviews":{
        "PK":"review_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "drivers":{
        "PK":"driver_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
    "menu_items":{
        "PK":"menu_item_id",
        "max_id":1,
        "min_id":1,
        "count":1,
        "is_cdc":True
    },
}
def run_func(func, *args, **kwargs):
    start_time = datetime.now()

    func(*args, **kwargs)

    end_time = datetime.now()

    duration = (end_time - start_time).total_seconds()

    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    tqdm.write(
        f"Duration: {int(hours):02d}:"
        f"{int(minutes):02d}:"
        f"{seconds:06.3f}"
    )
def read_postgres_table(table_name, sql_query):
 
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .options(**jdbc_properties) \
        .option("query", sql_query) \
        .load()
def read_postgres_table_parallel(sql_query, partition_column, lower, upper, num_partitions=4):
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", sql_query) \
        .option("partitionColumn", partition_column) \
        .option("lowerBound", lower) \
        .option("upperBound", upper) \
        .option("numPartitions", num_partitions) \
        .options(**jdbc_properties) \
        .load()




def get_metadata_tables(tables:dict, initial_load = False, is_initial_load_with_cdc = False):
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
            write.tqdm(f"Last generated batch timestamp: {last_generated_batch}")
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
        df = read_postgres_table(t, table_metadata_q)

        count = df.collect()[0][1]
        max_id = df.collect()[0][1]
        min_id = df.collect()[0][2]

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
    return tables


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
    old_batches_q = "select * from lake.bronze.batches_tracker where  status in ('pending', 'failed' {})"
    if tolerate_with_duplicate:
        old_batches_q = old_batches_q.format("")
        
    else:
        old_batches_q = old_batches_q.format(", 'success' ")
    df_old_batches = spark.sql(old_batches_q).cache()
    data = []
    pbar = tqdm(tables,desc="Inserting New Batches", leave=True)
    for tp in pbar:
        
        job_name = tp +'_'+load_type
        pbar.set_postfix(table=tp, job_name= job_name)
        
       

        all_batches = tables.get(tp).get("new_batches")
        
        status = "pending"
        pbar2 = tqdm(all_batches,desc="all batches", leave=True)
        for batch_number, lower_bound, upper_bound in pbar2:
            pbar2.set_postfix(batch_number=batch_number, lower_bound= lower_bound, upper_bound= upper_bound)
            same_prev_batch = df_old_batches.filter(F.col("job_name").contains(job_name) & 
                                                    ( ( F.col("lower_bound") == lower_bound) & 
                                                     ( F.col("upper_bound") == upper_bound)) )
            
            if  not same_prev_batch.isEmpty():
                status = same_prev_batch.select("status").distinct().collect()
                tqdm.write(f"this batch is exist in prev generateing for table {job_name}, batch_number {batch_number} and there status: {status}")
                #tqdm.write_as_df(same_prev_batch)
                continue
            data.append((str(uuid.uuid4()), job_name, batch_number, lower_bound, upper_bound, status, None, None, None, None, datetime.now())) 
    df_new_batches = spark.createDataFrame(data, batch_tracker_schema)
    df_new_batches.createOrReplaceTempView("new_batch_tracker")
    spark.sql(""" 
                    INSERT INTO TABLE lake.bronze.batches_tracker 
                        SELECT * FROM new_batch_tracker;
                    """)
    
    df_old_batches.persist()
    tqdm.write("inserting successed")


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
                                          num_partitions=4)
               
               added_kafka_columns = df.select(
                                    F.lit("inital_load_"+table).alias("kafka_topic"),
                                    F.lit(None).alias("kafka_partition"),
                                    F.lit(None).alias("kafka_offset"),
                                    F.lit(None).alias("kafka_timestamp"),                 
                                    F.lit('r').alias("operation"),
                                    F.lit(None).alias("before_payload"),
                                    F.to_json(F.struct([F.col(c) for c in df.columns])).alias("after_payload"),
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
                tqdm.write(e)
            
    
            

def run_pipeline(tables, initial_load = False, is_initial_load_with_cdc = False, tolerate_with_duplicate = False):
    try:
        if initial_load:
            tables_with_details = get_metadata_tables(tables, initial_load = initial_load, is_initial_load_with_cdc = is_initial_load_with_cdc)
            
            load_type = 'inital_load'
        else:
            tables_with_details = get_metadata_tables(tables, initial_load = initial_load, is_initial_load_with_cdc = is_initial_load_with_cdc)
            load_type = 'incremntial_load'
     
        if not tables_with_details and load_type == 'incremntial_load':
            print(f"There is no updates on the tables\n {tables_with_details}")
        elif not tables_with_details and load_type == 'inital_load':
            print(f"There is no details on the tables\n {tables_with_details}")
        tables_with_batches = generate_batches(tables_with_details)
        insert_new_batches(tables_with_batches, load_type = load_type, tolerate_with_duplicate=tolerate_with_duplicate)
        process_batches(tables_with_batches)
    except Exception as e: 
        print("="*50)
        print("Error while runing the pipeline:\n", e)
        print("="*50)


if __name__ == "__main__":
    print("Run Ingest to bronze pipeline")
    run_func(run_pipeline, initial_load = True, 
                            is_initial_load_with_cdc = True, 
                            tolerate_with_duplicate = False)