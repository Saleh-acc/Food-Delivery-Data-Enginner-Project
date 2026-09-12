


import warnings
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.sql.dataframe
from spark_jobs.shared.spark_session import get_spark


print(" Libraries imported successfully")
warnings.filterwarnings("ignore")


def flatten_kafka_payload(df:pyspark.sql.dataframe.DataFrame, table_schema) ->pyspark.sql.dataframe.DataFrame:
     #if isinstance(df, pyspark.sql.dataframe.DataFrame):
     
     
     
     return df.withColumn(
        "after_payload_value", F.from_json(F.col("after_payload"),table_schema)
                        ).select(
                                "*",
                                "after_payload_value.*",    
                            ).filter(
                                    (F.col("operation") != 'd') & (F.col("after_payload").isNotNull() 
                                                                )).drop("after_payload_value", "after_payload", "before_payload")
     
def fetch_bronze_layer_data(table_path_name, water_mark, spark = None):
    if not spark:
        spark =  get_spark("refresh spark - fetch bronze layer date")
    return spark.sql(f""" SELECT * 
                          FROM {table_path_name}
                          WHERE ingestion_ts >= CAST('{water_mark}' AS TIMESTAMP)""")