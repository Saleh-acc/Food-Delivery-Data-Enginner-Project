import warnings
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import * 
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
                                 customers_schema,
                                 restaurants_schema,
                                 zones_schema,
                                 cities_schema,
                                 
                                )
# from utils.postgres_config import PostgresConfig

# POSTGRES_CONFIG = PostgresConfig()._get_config_json()
# jdbc_url = POSTGRES_CONFIG.get("postgress").get("jdbc_url")
# jdbc_properties = POSTGRES_CONFIG.get("postgress").get("jdbc_properties")
# jdbc_properties =  {k: v for d in jdbc_properties for k, v in d.items()}
# bucket_name = POSTGRES_CONFIG.get("bucket").get("bronze-layer")

spark = (
    SparkSession
    .builder
    .appName("Transform data from bornze to silve, Batches")
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


def print_as_df(df, limit = 5):
    if isinstance(df, pyspark.sql.dataframe.DataFrame):
         display(df.limit(limit).toPandas())
    else:
        print('Unknow types. Just spark dataframe is acceptable')





def get_last_watermark(table_name:str, layer:str, prev_layer:str) -> datetime:
    last_watermark = datetime(2026, 5, 1) # default date
    try: 
        last_watermark = spark.sql(f"""
        SELECT COALESCE(
            (SELECT last_watermark FROM lake.{layer}.watermarks WHERE table_name = '{table_name}'),
            (SELECT MIN(ingestion_ts) FROM lake.{prev_layer}.{table_name})
        ) as last_watermark
    """).collect()[0][0]
        return last_watermark
    except Exception as e : 
        print(e, "\ndefault water_mark",last_watermark)
        raise


def flatten_kafka_payload(df:pyspark.sql.dataframe.DataFrame, table_schema):
     #if isinstance(df, pyspark.sql.dataframe.DataFrame):
     return df.withColumn(
        "after_payload_value", from_json(col("after_payload"),table_schema)
                        ).select(
                                "*",
                                "after_payload_value.*",    
                            ).filter(
                                    (col("operation") != 'd') & (col("after_payload").isNotNull() 
                                                                )).drop("after_payload_value", "after_payload", "before_payload")
     
def fetch_bronze_layer_data(table_path_name, water_mark):
    return spark.sql(f""" SELECT * 
                          FROM {table_path_name}
                          WHERE ingestion_ts >= CAST('{water_mark}' AS TIMESTAMP)""")



def update_watermark_table(table_name, layer_path = "lake.silver"):
    try:
        spark.sql(f"""
                    MERGE INTO {layer_path}.watermarks w
                    USING (SELECT '{table_name}' as table_name, CURRENT_TIMESTAMP as last_watermark, CURRENT_TIMESTAMP as updated_at) src
                    ON w.table_name = src.table_name
                    WHEN MATCHED THEN UPDATE SET w.last_watermark = src.last_watermark, w.updated_at = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN INSERT *
                    """)
        print(f"Updating the watermark table {layer_path}.{table_name} Successed ")
    except Exception as e:
        raise Exception(f"Could not update the water mark for {table_name}\n", e)

def run_func(func, *args, **kwargs):
    start_time = datetime.now()

    func(*args, **kwargs)

    end_time = datetime.now()

    duration = (end_time - start_time).total_seconds()

    print(f"Duration: {duration} sec")

def transforming_customers_to_silver(schema, table_name = 'customers'):

    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD3, CDC? No, transforming type: MERGE")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
       
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        df_menu_items_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema )
        # remove the duplicates rows 
        window = Window.partitionBy("customer_id").orderBy(col("source_lsn").desc())        
        df_cdc_after = df_menu_items_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')

        print("Is there Captured data from kafka?",not df_cdc_after.isEmpty())
        try:
            if  df_cdc_after.isEmpty():
                print("No data in df_canged to Upsert")
                return 
            df_cdc_after.createOrReplaceTempView("customers_updates")
            spark.sql("""
                            MERGE INTO lake.silver.customers cr
                            USING customers_updates cru
                                ON cr.customer_id = cru.customer_id
                            WHEN MATCHED AND (
                                                    cr.email != cru.email OR
                                                    cr.full_name != cru.full_name OR
                                                    cr.phone != cru.phone OR
                                                    cr.city_id != cru.city_id OR
                                                    cr.default_address != cru.default_address OR 
                                                    cr.is_active != cru.is_active
                                                ) THEN
                                UPDATE SET
                                   email = cru.email,
                                   full_name = cru.full_name,
                                   phone = cru.phone,
                                   city_id = cru.city_id,
                                   is_active = cru.is_active,
                                   signup_ts = cru.signup_date,
                                   created_at_ts =  cru.created_at,
                                   last_source_update_ts = cru.updated_at,
                                   prev_city_id = CASE WHEN cr.city_id != cru.city_id 
                                                      THEN cr.city_id ELSE cr.prev_city_id 
                                                  END ,
                                   prev_phone = CASE WHEN cr.phone != cru.phone 
                                                   THEN cr.phone ELSE cr.prev_phone 
                                                END,
                                   last_refresh_ts = CURRENT_TIMESTAMP
                           WHEN NOT MATCHED THEN
                               INSERT (
                                   customer_id,
                                   full_name ,
                                   email ,
                                   prev_phone ,
                                   phone ,
                                   city_id,
                                   prev_city_id ,
                                   is_active,
                                   default_address,
                                   signup_ts ,
                                   created_at_ts ,
                                   last_source_update_ts,
                                   last_refresh_ts 
                                    )
                                    VALUES (
                                        cru.customer_id,
                                        cru.full_name,
                                        cru.email,
                                        cru.prev_phone,
                                        cru.phone ,
                                        cru.city_id,
                                        cru.prev_city_id,
                                        cru.is_active,
                                        cru.default_address,
                                        cru.signup_date,
                                        cru.created_at,
                                        cru.updated_at,
                                        CURRENT_TIMESTAMP
                                    )
                                                            
                """)
            
        except Exception as e : 
                raise Exception(e ,f"could not UPSERT the data for table {table_name.replace('_', ' ')}")
        print(f"MERGE {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",df_cdc_after.count())
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_restaurants_to_silver(schema, table_name = 'restaurants'):
    
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD2, CDC? No, transforming type: MERGE")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )       
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        #df_row_data_bronze_layer = df_test
        df_menu_items_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema )
        
        # remove the duplicates rows 
        window = Window.partitionBy("restaurant_id").orderBy(col("source_lsn").desc())        
        df_cdc = df_menu_items_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
        
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
        
        df_changed.createOrReplaceTempView("restaurants_merge")
        print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())
        print("  Is there data in the silver layer?",not df_silver_current.isEmpty())
        print("  Is there data after left join df_cdc with df_silver_current?",not df_changed.isEmpty())
        new_rows = df_changed.count()
        try:
            try:
                if  df_changed.isEmpty():
                    print("  No data in df_canged to Upsert")
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
            
            print(f"MERGE {table_name} data from bronze --> silver: successed")
            print("  total of new inserts",new_rows)
            update_watermark_table(table_name = table_name)
            print("  water mark have been updated will be >", last_watermark)
        except Exception as e : 
            raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_zones_to_silver(schema, table_name = 'zones'):
    
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD0, CDC? No, transforming type: OVERWRITE")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
       
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        #df_row_data_bronze_layer = df_test
        df_menu_items_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema )
        
        # remove the duplicates rows 
        window = Window.partitionBy("zone_id").orderBy(col("source_lsn").desc())        
        df_cdc = df_menu_items_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
        
        
        df_cdc.createOrReplaceTempView("zones_updated")
        print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())

        new_rows = df_cdc.count()
        try:
            if  df_cdc.isEmpty():
                print("  No data in df_canged to OVERWRITE")
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
            raise Exception(f"Error occred in function overwrite the {table_name.replace('_', ' ')} rows", e)
        
        print(f"OVERWRITE {table_name} data from bronze --> silver: successed")
        print("  total of new inserts or updated",new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_cities_to_silver(schema, table_name = 'cities'):
    
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD0, CDC? No, transforming type: OVERWRITE")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
       
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        #df_row_data_bronze_layer = df_test
        df_cities_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema )
        
        # remove the duplicates rows 
        window = Window.partitionBy("city_id").orderBy(col("source_lsn").desc())        
        df_cdc = df_cities_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
        
        
        df_cdc.createOrReplaceTempView("cities_updated")
        print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())

        new_rows = df_cdc.count()
        try:
            if  df_cdc.isEmpty():
                print("  No data in df_canged to OVERWRITE")
                return
    # city_id              STRING, 
    # city_name            STRING,
    # country_code         STRING,
    # timezone             STRING,
    # created_at           TIMESTAMP,
    # updated_at TIMESTAMP
            spark.sql("""
                        MERGE INTO lake.silver.cities c
                        USING cities_updated cu
                             on c.city_id = cu.city_id
                        WHEN MATCHED THEN UPDATE SET 
                                city_name = cu.city_name,
                                country_code = cu.country_code,
                                timezone = cu.timezone,
                                created_at_ts = cu.created_at,
                                last_source_update_ts =  cu.updated_at,
                                last_refresh_ts =  CURRENT_TIMESTAMP
                        WHEN NOT MATCHED THEN 
                            INSERT (
                                city_id,
                                city_name,
                                country_code,
                                timezone,
                                created_at_ts,
                                last_source_update_ts,
                                last_refresh_ts
                            )
                            VALUES (
                                cu.city_id,
                                cu.city_name,
                                cu.country_code,
                                cu.timezone,
                                cu.created_at,
                                cu.updated_at,
                                CURRENT_TIMESTAMP
                            )
                        """)

        
        except Exception as e:
            raise Exception(f"Error occred in function overwrite the {table_name.replace('_', ' ')} rows", e)
        
        print(f"OVERWRITE {table_name} data from bronze --> silver: successed")
        print("  total of new inserts or updated",new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def main():
    print("="*50)
    run_func(transforming_customers_to_silver, customers_schema)
    print("="*50)
    run_func(transforming_restaurants_to_silver, restaurants_schema)
    print("="*50)
    run_func(transforming_zones_to_silver, zones_schema)
    print("="*50)
    run_func(transforming_cities_to_silver, cities_schema)
    print("="*50)    

    print("Transformation from bronze to silver, Batching, Successed!!")
if __name__ == "__main__":
    print("Run bronze to silver pipeline")
    main()
