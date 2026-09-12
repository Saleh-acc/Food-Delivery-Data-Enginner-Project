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
    
    return df_raw, last_watermark