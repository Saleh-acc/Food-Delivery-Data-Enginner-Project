import warnings
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.dataframe
from datetime import datetime
from IPython.display import display
from tqdm import tqdm

import pandas as pd
import sys
import os
import json
sys.path.append(os.path.abspath("/opt/spark-apps"))
print("="*50)
print(os.path.abspath("/opt/spark-apps/"))
print("="*50)
from ddl.tables_schema import  (
                                orders_schema, 
                                order_itmes_schema, 
                                payments_schema, 
                                order_status_events_schema,
                                menu_items_schema,
                                drivers_items_schema,
                                reviews_schema
                                )

# to get the currnet path root for apps/ to call module there 



print(" Libraries imported successfully")
warnings.filterwarnings("ignore")

spark = (
    SparkSession
    .builder
    .appName("Transform data from bronze to silver, Streaming")
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

    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    tqdm.write(
        f"Duration: {int(hours):02d}:"
        f"{int(minutes):02d}:"
        f"{seconds:06.3f}"
    )

def transforming_orders_to_silver(json_schema, table_name = "orders"):

    print(f"Start transforming \ntable name: {table_name}, table type: FACT, CDC? Yes, transforming type: UPSERT")
    try:
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
        
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        
        df_flattened = flatten_kafka_payload(df = df_row_data_bronze_layer, 
                                             table_schema = json_schema)
        window = Window.partitionBy("order_id").orderBy(col("source_lsn").desc())
    
        # remove duplicate rows 
        df_flattened_partitioned = df_flattened.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')

        if df_flattened_partitioned.isEmpty():
            print(f"the bronze layer data is Empty!! for table {table_name} last water mark {last_watermark}")
            return 
        df_flattened_partitioned = df_flattened_partitioned.select(
                    "order_id",
                    "customer_id",
                    "restaurant_id",
                    "driver_id",
                    "pickup_zone_id",
                    "dropoff_zone_id",
                    "status",
                    "subtotal" ,
                    "delivery_fee",
                    "service_fee",
                    "discount",  
                    "tip",
                    "total",
                       col("placed_at").alias("placed_at_ts"), 
                        col("confirmed_at").alias("confirmed_at_ts"),
                        col("ready_at").alias("ready_at_ts"),
                        col("picked_up_at").alias("picked_up_at_ts"),
                        col("delivered_at").alias("delivered_at_ts"),
                        col("cancelled_at").alias("cancelled_at_ts"),
                        col("created_at").alias("created_at_ts"),
                        col("updated_at").alias("updated_at_ts")
        )

        df_flattened_partitioned.createOrReplaceTempView("silver_orders_updates")
        source = spark.table("silver_orders_updates").alias("s")
        target = spark.table("lake.silver.orders").alias("t")
        updates = (
            source
            .join(target, on="order_id", how="left")
            .withColumn(
                "action",
                when(col("t.order_id").isNotNull(), "update")
                .otherwise("insert")
            )
        )

        # if the load is havy use .cache() then .persist to release  the memory 
        updates.groupBy("action").count().show()
        spark.sql("""
            MERGE INTO lake.silver.orders od
            USING silver_orders_updates s
            ON od.order_id = s.order_id
            
            WHEN MATCHED THEN UPDATE SET
                od.customer_id           = s.customer_id,
                od.restaurant_id         = s.restaurant_id,
                od.driver_id             = s.driver_id,
                od.pickup_zone_id        = s.pickup_zone_id,
                od.dropoff_zone_id       = s.dropoff_zone_id,
                od.status                = s.status,
                od.subtotal              = s.subtotal,
                od.delivery_fee          = s.delivery_fee,
                od.service_fee           = s.service_fee,
                od.discount              = s.discount,
                od.tip                   = s.tip,
                od.total                 = s.total,
                od.placed_at_ts          = s.placed_at_ts,
                od.confirmed_at_ts       = s.confirmed_at_ts,
                od.ready_at_ts           = s.ready_at_ts,
                od.picked_up_at_ts       = s.picked_up_at_ts,
                od.delivered_at_ts       = s.delivered_at_ts,
                od.cancelled_at_ts       = s.cancelled_at_ts,
                od.created_at_ts         = s.created_at_ts,
                od.last_source_update_ts = s.updated_at_ts,        
                od.last_refresh_ts  = CURRENT_TIMESTAMP
            
            WHEN NOT MATCHED THEN INSERT (
                order_id,
                customer_id,
                restaurant_id,
                driver_id,
                pickup_zone_id,
                dropoff_zone_id,
                status,
                subtotal,
                delivery_fee,
                service_fee,
                discount,
                tip,
                total,
                placed_at_ts,
                confirmed_at_ts,
                ready_at_ts,
                picked_up_at_ts,
                delivered_at_ts,
                cancelled_at_ts,
                created_at_ts,
                last_source_update_ts,
                last_refresh_ts
            )
            VALUES (
                s.order_id,
                s.customer_id,
                s.restaurant_id,
                s.driver_id,
                s.pickup_zone_id,
                s.dropoff_zone_id,
                s.status,
                s.subtotal,
                s.delivery_fee,
                s.service_fee,
                s.discount,
                s.tip,
                s.total,
                s.placed_at_ts,
                s.confirmed_at_ts,
                s.ready_at_ts,
                s.picked_up_at_ts,
                s.delivered_at_ts,
                s.cancelled_at_ts,
                s.created_at_ts,
                s.updated_at_ts,
                CURRENT_TIMESTAMP          -- last_refresh_date_ts
            )
            """)
        print("MERGE orders data from bronze --> silver: successed")
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e + "\n transforming orders from bronze --> silver: failed")

def transforming_order_items_to_silver(order_itmes_schema, table_name='order_items'):

    print(f"Start transforming\ntable name: {table_name}, table type: FACT, CDC? Yes, transforming type: APPEND")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
    
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        df_order_items_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = order_itmes_schema ).dropDuplicates(['order_item_id', 'source_lsn'])

        df_order_items_ids = spark.sql("""
                    SELECT  oditm.order_item_id  
                    FROM lake.silver.order_items oditm
                 """
                 )
        df_order_items_joined =  df_order_items_flatern.join(
                                        df_order_items_ids,
                                        on="order_item_id", how='left_anti'
                                        )
        print("  Is there data in the silver layer?",not df_order_items_ids.isEmpty())
        print("  Is there data after left join the bronze layer with silver layer?",not df_order_items_joined.isEmpty())
        if df_order_items_joined.isEmpty():
            print(f"the bronze layer data is Empty!! for table {table_name} last water mark {last_watermark}")
            return 
        df_order_items_joined = df_order_items_joined.withColumnRenamed("updated_at", "last_source_update_ts")
        df_order_items_joined = df_order_items_joined.withColumn("last_refresh_ts", current_timestamp())
        df_order_items_joined = df_order_items_joined.select(
                                        "order_item_id",
                                        "order_id",
                                        "menu_item_id",
                                        "quantity",
                                        "unit_price",
                                        "line_total",
                                        "created_at"  ,
                                        "last_source_update_ts",
                                        "last_refresh_date_ts" 
        )
        df_order_items_joined.createOrReplaceTempView("order_items_apending")
        count_new_rows = df_order_items_joined.count()

        spark.sql("""
                  INSERT INTO lake.silver.order_items 
                  SELECT * FROM order_items_apending
                  """)
        print(f"APPEND {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",count_new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exceptiona as e : 
        raise Exception(e + f"\n transforming {table_name} from bronze --> silver: failed")

def transforming_payments_to_silver(payments_schema, table_name = 'payments'):
    
    print(f"Start transforming \ntable name: {table_name}, table type: FACT, CDC? Yes, transforming type: MERGE")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
    
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        df_payments_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = payments_schema )

        window = Window.partitionBy("payment_id").orderBy(col("source_lsn").desc())
            
        df_payments_partitioned = df_payments_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
        print("  Is there new data in bronze layer?",not df_row_data_bronze_layer.isEmpty())
       
        if df_payments_partitioned.isEmpty():
            print(f"the bronze layer data is Empty!! for table {table_name} last water mark {last_watermark}")
            return 
        df_payments_partitioned = df_payments_partitioned.select(
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
        df_payments_partitioned = df_payments_partitioned.withColumnRenamed("updated_at", "last_source_update_ts")
        df_payments_partitioned = df_payments_partitioned.withColumn("last_refresh_date_ts", current_timestamp())
        df_payments_partitioned.createOrReplaceTempView("silver_payment_updates")
        
        source = spark.table("silver_payment_updates").alias("s")
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
    
        print("  Is there data after left join the bronze layer with silver layer?",not updates.isEmpty())
        updates.groupBy("action").count().show()
        
        spark.sql("""
            MERGE INTO lake.silver.payments od
            USING silver_payment_updates s
            ON od.payment_id = s.payment_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """)
        print(f" APPEND {table_name} data from bronze --> silver: successed")
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exceptiona as e : 
        raise Exception(e + f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_order_status_events_to_silver(schema, table_name = "order_status_events"):
    

    print(f"Start transforming \ntable name: {table_name}, table type: FACT, CDC? Yes, transforming type: APPEND")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
    
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        df_order_status_events_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema ).dropDuplicates(['event_id', 'source_lsn'])
        df_order_status_events_ids = spark.sql("""
                SELECT  oditm.event_id  
                FROM lake.silver.order_status_events oditm
             """
             )
    
        df_order_status_events_joined = df_order_status_events_flatern.join(
                                    df_order_status_events_ids,
                                    on="event_id", how='left_anti'
                                    )
        print("  Is there data in the silver layer?",not df_order_status_events_ids.isEmpty())
        print("  Is there data after left join the bronze layer with silver layer?",not df_order_status_events_joined.isEmpty())
        if df_order_status_events_joined.isEmpty():
            print(f"the bronze layer data is Empty!! for table {table_name} last water mark {last_watermark}")
            return 

                
        df_order_status_events_joined = df_order_status_events_joined.withColumn("last_refresh_date_ts", current_timestamp())
        df_order_status_events_joined = df_order_status_events_joined.select(
                                        "event_id",
                                        "order_id",
                                        "from_status",
                                        "to_status",
                                        "event_ts",
                                        "actor_type"  ,
                                        "actor_id",
                                        "notes",
                                        "created_at",
                                        "last_refresh_date_ts" 
        )#.cache() # use it if needed 
        df_order_status_events_joined.createOrReplaceTempView("order_status_events_apending")
        count_new_rows = df_order_status_events_joined.count()
        spark.sql("""
                  INSERT INTO lake.silver.order_status_events 
                  SELECT * FROM order_status_events_apending
                  """)
        #df_order_status_events_joined.unpersist()
    
        print(f"APPEND {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",count_new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_reviews_to_silver(schema, table_name = "reviews"):

    
    print(f"Start transforming \ntable name: {table_name}, table type: FACT, CDC? Yes, transforming type: APPEND")
    try:    
        
        last_watermark = get_last_watermark(table_name,
                                            layer='silver',
                                            prev_layer='bronze'
                                           )
    
        df_row_data_bronze_layer = fetch_bronze_layer_data(table_path_name = "lake.bronze."+table_name,
                                                            water_mark=last_watermark
                                                          )
        df_reviews_flatern = flatten_kafka_payload(df = df_row_data_bronze_layer,
                                                       table_schema = schema ).dropDuplicates(['review_id', 'source_lsn'])
        
        df_reviews_ids = spark.sql("""
                    SELECT  rv.review_id  
                    FROM lake.silver.reviews rv
                                 """
                                 )
        
        df_reviews_joined = df_reviews_flatern.join(
                                        df_reviews_ids,
                                        on="review_id", how='left_anti'
                                        )
        print("  Is there data in the silver layer?",not df_reviews_ids.isEmpty())
        print("  Is there data after left join the bronze layer with silver layer?",not df_reviews_joined.isEmpty())
        if df_reviews_joined.isEmpty():
            print(f"the bronze layer data is Empty!! for table {table_name} last water mark {last_watermark}")
            return 
        df_reviews_joined = df_reviews_joined.withColumn("last_refresh_date_ts", current_timestamp())
        df_reviews_joined = df_reviews_joined.select(
                                        "review_id",
                                        "order_id",
                                        "customer_id",
                                        "restaurant_id",
                                        "driver_id",
                                        "food_rating"  ,
                                        "delivery_rating",
                                        "comment",
                                        "submitted_at",
                                        "last_refresh_date_ts" 
        )#.cache()
        df_reviews_joined.createOrReplaceTempView("reviews_apending")
        count_new_rows = df_reviews_joined.count()
        spark.sql("""
                  INSERT INTO lake.silver.reviews 
                  SELECT * FROM reviews_apending
                  """)
       # df_reviews_joined.unpersist()    
        
        print(f"APPEND {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",count_new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")


def transforming_menu_items_to_silver(schema, table_name = 'menu_items'):
    
    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD2, CDC? Yes, transforming type: MERGE")
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
        window = Window.partitionBy("menu_item_id").orderBy(col("source_lsn").desc())        
        df_cdc = df_menu_items_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
        #print_as_df(df_cdc)
        df_silver_current = spark.sql("SELECT * FROM lake.silver.menu_items WHERE is_current = true")
        df_changed = df_cdc.join(
            df_silver_current,
            on="menu_item_id",
            how="left"
        ).filter(
           df_silver_current.menu_item_id.isNull() |
            (df_cdc.updated_at != df_silver_current.last_source_update_ts)
        ).select(df_cdc["*"])#.cache()
        
        df_changed.createOrReplaceTempView("menu_items_merge")
        print("  Is there Caputred data from Kafka?",not df_cdc.isEmpty())
        print("  Is there data in the silver layer?",not df_silver_current.isEmpty())
        print("  Is there data after left join df_cdc with df_silver_current?",not df_changed.isEmpty())
        new_rows = df_changed.count()
        try:
            if  df_changed.isEmpty():
                print("  No data in df_canged to Upsert")
                return
            spark.sql("""
                        MERGE INTO lake.silver.menu_items mitm
                        USING menu_items_merge mitm_updated
                             on mitm.menu_item_id = mitm_updated.menu_item_id
                             AND mitm.is_current = True
                        WHEN MATCHED THEN UPDATE SET 
                                            mitm.is_current = False,
                                            mitm.eff_end_ts = CURRENT_TIMESTAMP,
                                            mitm.last_source_update_ts = mitm_updated.updated_at
                        """)
        
            spark.sql("""
                        INSERT INTO lake.silver.menu_items SELECT 
                                        menu_item_id,
                                        restaurant_id,
                                        name,
                                        category,
                                        price,
                                        is_available,
                                        created_at,
                                        CURRENT_TIMESTAMP,
                                        NULL,
                                        True,
                                        updated_at,
                                        CURRENT_TIMESTAMP  
                                    FROM menu_items_merge mitm_updated              
                        """)
        
        except Exception as e:
            raise Exception("Error occred in function merge the menue items rows", e)
        
        print(f"MERGE {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",new_rows)
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")

def transforming_drivers_to_silver(schema, table_name = 'drivers'):

    print(f"Start transforming \ntable name: {table_name}, table type: DIM: SCD3, CDC? Yes, transforming type: MERGE")
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
        window = Window.partitionBy("driver_id").orderBy(col("source_lsn").desc())        
        df_cdc_after = df_menu_items_flatern.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop('rn')
        
        print("Is there Captured data from kafka?",not df_cdc_after.isEmpty())
        try:
            if  df_cdc_after.isEmpty():
                print("No data in df_canged to Upsert")
                return 
            df_cdc_after.createOrReplaceTempView("drivers_updates")
            
            spark.sql("""
                            MERGE INTO lake.silver.drivers dr
                            USING drivers_updates dru
                                ON dr.driver_id = dru.driver_id
                            WHEN MATCHED AND (
                                                    dr.city_id != dru.city_id OR
                                                    dr.vehicle_type != dru.vehicle_type OR
                                                    dr.status != dru.status OR
                                                    dr.phone != dru.phone OR
                                                    dr.is_active != dru.is_active
                                                ) THEN
                                UPDATE SET
                                   full_name = dru.full_name,
                                   phone = dru.phone,
                                   vehicle_type = dru.vehicle_type,
                                   city_id = dru.city_id,
                                   status = dru.status,
                                   is_active = dru.is_active,
                                   onboarded_at_ts = dru.onboarded_at,
                                   created_at_ts =  dru.created_at,
                                   prev_vehicle_type = CASE WHEN dr.vehicle_type != dru.vehicle_type THEN dr.vehicle_type ELSE dr.prev_vehicle_type END ,
                                   prev_city_id = CASE WHEN dr.city_id != dru.city_id THEN dr.city_id ELSE dr.prev_city_id END,
                                   last_source_update_ts = dru.updated_at,
                                   last_refresh_ts = CURRENT_TIMESTAMP
                           WHEN NOT MATCHED THEN
                               INSERT (
                                        driver_id,
                                        full_name,
                                        phone,
                                        vehicle_type,
                                        city_id,
                                        status,
                                        is_active,
                                        onboarded_at_ts,
                                        created_at_ts,
                                        prev_vehicle_type,
                                        prev_city_id,
                                        last_refresh_ts,
                                        last_source_update_ts
                                    )
                                    VALUES (
                                        dru.driver_id,
                                        dru.full_name,
                                        dru.phone,
                                        dru.vehicle_type,
                                        dru.city_id,
                                        dru.status,
                                        dru.is_active,
                                        dru.onboarded_at,
                                        dru.created_at,
                                        NULL,
                                        NULL,
                                        CURRENT_TIMESTAMP,
                                        dru.updated_at
                                    )
                                                            
                """)
            
        except Exception as e : 
                raise Exception(e ,f"could not UPSERT the data for table {table_name}")
        print(f"MERGE {table_name} data from bronze --> silver: successed")
        print("  total of new inserts",df_cdc_after.count())
        update_watermark_table(table_name = table_name)
        print("  water mark have been updated will be >", last_watermark)
    except Exception as e : 
        raise Exception(e ,f"\n transforming {table_name} from bronze --> silver: failed")
        
def main():
    print("="*50)
    run_func(transforming_orders_to_silver, orders_schema)
    print("="*50)
    run_func(transforming_order_items_to_silver, order_itmes_schema)
    print("="*50)
    run_func(transforming_payments_to_silver, payments_schema)
    print("="*50)
    run_func(transforming_order_status_events_to_silver, order_status_events_schema)
    print("="*50)    
    run_func(transforming_reviews_to_silver, reviews_schema)
    print("="*50)
    run_func(transforming_menu_items_to_silver, menu_items_schema)
    print("="*50)
    run_func(transforming_drivers_to_silver, drivers_items_schema)
    print("="*50)
    print("Transformation from bronze to silver, Successed!!")
if __name__ == "__main__":
    print("Run bronze to silver pipeline")
    main()













