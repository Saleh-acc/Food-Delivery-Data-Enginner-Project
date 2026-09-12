from datetime import datetime
from spark_jobs.shared.spark_session import get_spark


def get_last_watermark(table_name:str, layer:str, prev_layer:str, spark = None):

    if not spark:
        spark = get_spark("refresh spark  - get last watermark")
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
    
    
def update_watermark_table(table_name, layer_path = "lake.silver", spark=None):
    
    if not spark:
        spark = get_spark("refresh spark  - update watermark table")
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