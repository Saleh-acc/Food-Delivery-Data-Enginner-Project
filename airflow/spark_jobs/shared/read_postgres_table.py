from spark_jobs.shared.spark_session import get_spark
from spark_jobs.utils.postgres_config import PostgresConfig

POSTGRES_CONFIG = PostgresConfig(config_path =  "/opt/airflow/config/batches_mapping.yaml")._get_config_json()
jdbc_url = POSTGRES_CONFIG.get("postgress").get("jdbc_url")
jdbc_properties = POSTGRES_CONFIG.get("postgress").get("jdbc_properties")
jdbc_properties =  {k: v for d in jdbc_properties for k, v in d.items()}
bucket_name = POSTGRES_CONFIG.get("bucket").get("bronze-layer")

def read_postgres_table(sql_query, 
                        jdbc_url, 
                        jdbc_properties, 
                        spark_session = None):
    
    if not spark_session:
        spark_session = get_spark("fetching data from postgress")
    return spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .options(**jdbc_properties) \
            .option("query", sql_query) \
            .load()
        
def read_postgres_table_parallel(sql_query,
                                 jdbc_url,
                                 jdbc_properties, 
                                 partition_column, 
                                 lower, 
                                 upper, 
                                 num_partitions=4, 
                                 spark_session = None,):
    if not spark_session:
        spark_session = get_spark("fetching data from postgress")
    return spark_session.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", sql_query) \
        .option("partitionColumn", partition_column) \
        .option("lowerBound", lower) \
        .option("upperBound", upper) \
        .option("numPartitions", num_partitions) \
        .options(**jdbc_properties) \
        .load()