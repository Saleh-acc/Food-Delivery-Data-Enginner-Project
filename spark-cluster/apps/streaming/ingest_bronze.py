import warnings
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import * # type: ignore
from pyspark.sql.types import * # type: ignore
import yaml
from pathlib import Path
import sys
import os
# to get the currnet path root for apps/ to call moudle there 
sys.path.append(os.path.abspath("/opt/spark-apps"))
print("="*50)
print(os.path.abspath("/opt/spark-apps/"))
print("="*50)
from utils.kafka_config import KafkaConfig



print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
CONFIG = KafkaConfig()._get_config_json()

TOPIC_TO_TABLES = CONFIG.get("topic_to_table")
KAFAK_BOOTSTRAP_SERVER = CONFIG.get("kafka").get("bootstrap_servers")
SUBSCRIBE_PATTERN = CONFIG.get("kafka").get("subscribe_pattern")
TRIGGER_INTERVAL = CONFIG.get("streaming").get("trigger_interval")
CHECKPOINT_LOCATION = CONFIG.get("streaming").get("checkpoint_location")
STARTING_OFFSETS = CONFIG.get("streaming").get("starting_offsets")

spark = (
    SparkSession
    .builder
    .appName("Streaming from Kafka")
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
    .config(
      "spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION
    )
#    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFAK_BOOTSTRAP_SERVER)
        .option("subscribePattern", SUBSCRIBE_PATTERN)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
)
schema_source = schema = StructType([# type: ignore

    StructField("ts_ms", LongType(), True),# type: ignore
    StructField("lsn", LongType(), True),# type: ignore
    StructField("txId", LongType(), True),# type: ignore
    StructField("table", StringType(), True),# type: ignore
    StructField("snapshot", StringType(), True),# type: ignore

])
schema = StructType([# type: ignore

    StructField("op", StringType(), True), # pyright: ignore[reportUndefinedVariable]
    StructField("before", StringType(), True), # type: ignore
    StructField("after", StringType(), True),# type: ignore
    StructField("source", schema_source, True), # type: ignore
    StructField("ts_ms", LongType(), True),# type: ignore
])

kafka_json_df = kafka_df.withColumn("value", expr("cast(value as string)"))# type: ignore
parsed_kafka = kafka_json_df.withColumn("value_json", from_json(col("value"), schema))# type: ignore
flatened_event = parsed_kafka.select(
                            col("topic").alias("kafka_topic"),# type: ignore
                            col("partition").alias("kafka_partition"),# type: ignore
                            col("offset").alias("kafka_offset"),# type: ignore
                            col("timestamp").alias("kafka_timestamp"),          # type: ignore                      
                            col("value_json.op").alias("operation"),# type: ignore
                            col("value_json.before").alias("before_payload"),# type: ignore
                            col("value_json.after").alias("after_payload"),# type: ignore
                            col("value_json.source.ts_ms").alias("source_ts_ms"),# type: ignore
                            col("value_json.source.lsn").alias("source_lsn"),# type: ignore
                            col("value_json.source.txId").alias("source_txid"),# type: ignore
                            col("value_json.source.table").alias("source_table"),# type: ignore
                            col("value_json.source.snapshot").alias("source_snapshot"),# type: ignore
                            current_timestamp().alias("ingestion_ts")# type: ignore
                    )



def write_to_bronze(df, batch_id):
    topic_list = [r["kafka_topic"] for r in df.select("kafka_topic").distinct().collect()]
    kafka_offset_max = df.select(max("kafka_offset")).collect()
    
    for t in topic_list:
        table_path = TOPIC_TO_TABLES.get(t)
        print(f"table_path: {table_path}, Batch id:", str(batch_id))
        print(f"kafka current offset:",kafka_offset_max)
        df_filtered = df.filter(col("kafka_topic") == t)# type: ignore
        if df_filtered.isEmpty():
            print(f"Unknown topic: {t}, skipping")
            continue
        (
            df_filtered
                .writeTo(table_path)
                .append()
        )
        df_filtered.show(5, truncate=False)
    
(flatened_event.writeStream
                .foreachBatch(write_to_bronze)
                .outputMode("append")
                .trigger(processingTime = TRIGGER_INTERVAL)
                .start()
                .awaitTermination()
                 

        )                      

# Run this file 
#  docker exec spark-master /opt/spark/bin/spark-submit `  --master spark://spark-master:7077 `  /opt/spark-apps/streaming/ingest_bronze.py

