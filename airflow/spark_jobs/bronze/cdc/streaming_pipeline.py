import warnings
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import sys
import argparse
import os
from spark_jobs.shared.spark_session import get_spark_streaming
from spark_jobs.utils.kafka_config import KafkaConfig

sys.path.append(os.path.abspath("/opt/spark-apps"))
print("="*50)
print(os.path.abspath("/opt/spark-apps/"))
print("="*50)
from spark_jobs.ddl.tables_schema import  (
                                kafka_schema
                                )

# to get the currnet path root for apps/ to call module there 

        
        
    

print(" Libraries imported successfully")
warnings.filterwarnings("ignore")
CONFIG = KafkaConfig()._get_config_json()

TOPIC_TO_TABLES = CONFIG.get("topic_to_table")
KAFAK_BOOTSTRAP_SERVER = CONFIG.get("kafka").get("bootstrap_servers")
SUBSCRIBE_PATTERN = CONFIG.get("kafka").get("subscribe_pattern")
TRIGGER_INTERVAL = CONFIG.get("streaming").get("trigger_interval")
CHECKPOINT_LOCATION = CONFIG.get("streaming").get("checkpoint_location")
STARTING_OFFSETS = CONFIG.get("streaming").get("starting_offsets")



def preprocess_kafka(kafka_df, schema):
    kafka_json_df = kafka_df.withColumn("value", expr("cast(value as string)"))# type: ignore
    parsed_kafka = kafka_json_df.withColumn("value_json", from_json(col("value"), schema))# type: ignore
    return parsed_kafka.select(
                                col("topic").alias("kafka_topic"),# type: ignore
                                col("partition").alias("kafka_partition"),# type: ignore
                                col("offset").alias("kafka_offset"),# type: ignore
                                 from_utc_timestamp(col("timestamp"), "Asia/Riyadh").alias("kafka_timestamp"),
                             #   col("timestamp").alias("kafka_timestamp"),          # type: ignore                      
                                col("value_json.op").alias("operation"),# type: ignore
                                col("value_json.before").alias("before_payload"),# type: ignore
                                col("value_json.after").alias("after_payload"),# type: ignore
                                col("value_json.source.ts_ms").alias("source_ts_ms"),# type: ignore
                                col("value_json.source.lsn").alias("source_lsn"),# type: ignore
                                col("value_json.source.txId").alias("source_txid"),# type: ignore
                                col("value_json.source.table").alias("source_table"),# type: ignore
                                col("value_json.source.snapshot").alias("source_snapshot"),# type: ignore
                                from_utc_timestamp(current_timestamp(), "Asia/Riyadh").alias("ingestion_ts")# type: ignore
                        )


def read_data(spark,topic):
    topic = f"cdc.public.{topic}"
    find_topic = TOPIC_TO_TABLES.get(topic)
    print(f"listion to {find_topic}")
    if not find_topic:
        raise ValueError(f"The topic not exist {topic}")
    return  (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFAK_BOOTSTRAP_SERVER)

        # ── Offset behavior ───────────────────────────────────────
        # earliest: read from beginning (use for initial load)
        # latest:   read only new messages (use for ongoing streaming)
        .option("startingOffsets", "earliest")
        # ── Backpressure ──────────────────────────────────────────
        # Max records per trigger across all partitions.
        # Prevents OOM on large backlogs during initial load.
        #.option("maxOffsetsPerTrigger", "10000")
        # ── Fault tolerance ───────────────────────────────────────
        # If true, missing/deleted topics cause the query to fail
        # fast rather than silently skip data.
        .option("failOnDataLoss", "false")
        #.option("subscribePattern",'cdc.public.*')
        .option("subscribePattern", "cdc\\.public\\..*")
        .load()
    )
def write_to_bronze(df, batch_id):
    topic_list = [r["kafka_topic"] for r in df.select("kafka_topic").distinct().collect()]
    kafka_offset_max = df.select(max("kafka_offset")).collect()
    elapsed = time.time() - start_time
    remaining = args.duration - elapsed
    print(f"Batch {batch_id} done. Elapsed: {elapsed:.0f}s, Remaining: {remaining:.0f}s")

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
        print("3- data writen successfully, total rows inserted",df_filtered.count())
    


if __name__ == "__main__":
    print("Ingest data from source to bronze layer, Streaming")
     

    # tables = dict()
    # json_path = Path(__file__).parent / "tables.json"
    # with open(json_path, "r") as f:
    #     tables = json.load(f)
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--config")

    spark =  get_spark_streaming(app_name="Ingest from kafka to bronze layer - Streaming ",
                                 check_point_path=CHECKPOINT_LOCATION
                                 )

    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    access_key = hadoop_conf.get("fs.s3a.access.key")
    secret_key = hadoop_conf.get("fs.s3a.secret.key")
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, required=True, default='30')
    parser.add_argument("--topics", required=True)
    # parser.add_argument("--prev_state_path", required=True, default="False")
    args = parser.parse_args()
    # list_tables = args.config if isinstance(args.config, list) else [args.config]
    # print(list_tables)
    # running_tables = {
    #                     name: tables[name]
    #                     for name in list_tables
    #                         }  
    # print(f"the extracting tables:{running_tables}")
    global start_time
    global duration
     
    start_time = time.time()
    duration = args.duration
    # check remaining time after each batch
    elapsed = time.time() - start_time
    remaining = args.duration - elapsed
    raw_data_df = read_data(spark=spark,
                            topic= args.topics
                            )
    print("1- raw data read successfully")
    processd_df = preprocess_kafka(raw_data_df,
                                   schema=kafka_schema)
    print("2- data processd successfully ")
    q = (processd_df.writeStream
                .foreachBatch(write_to_bronze)
                .outputMode("append")
                .trigger(processingTime = TRIGGER_INTERVAL)
                .start() 
        ) 
    q.awaitTermination(timeout=args.duration)
    q.stop()
    elapsed = time.time() - start_time
    print(f"Streaming stopped. Total elapsed: {elapsed:.0f}s")   
    