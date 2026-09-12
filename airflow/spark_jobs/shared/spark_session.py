from pyspark.sql import SparkSession, Window
import pyspark

def get_spark(app_name:str, 
              partitions:int = 1, ) -> pyspark.sql.session.SparkSession:
    return (
        SparkSession
        .builder
        .appName(app_name)
        .config("spark.streaming.stopGracefullyOnShutdown", True)
        .config("spark.sql.shuffle.partitions", partitions)
        # .config(
        #     "spark.jars.packages",
        #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
        # )
        # .config(
        #     "spark.sql.debug.maxToStringFields", "1000"
        # )
        .config(
            "spark.sql.adaptive.enabled", "true"
        )
        
        .config("spark.driver.host", "airflow-airflow-worker-1"
        )
        .config("spark.driver.bindAddress", "0.0.0.0"
        )
        .config(
                "spark.driver.port", "40001"
        )
        .config(
                "spark.driver.port", "40003"
        )
        .config(
            "spark.blockManager.port", "40002"
        )
        .config(
            "spark.blockManager.port", "40004"
        )
        
        # .config("spark.sql.execution.pyspark.udf.validatePythonVersion", "false")
        # .config(
        #   "spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION
        # )
    #    .master("spark://spark-master:7077")
        .getOrCreate()
    )
def get_spark_streaming(app_name:str = 'Transform data from bronze to silver, Streaming', 
                        check_point_path:str = "s3a://lake/spark/checkpoints/bronze_writer",
                        partitions:int = 4
                        ) -> pyspark.sql.session.SparkSession :
    
    
    return  (
                SparkSession
                .builder
                .appName(app_name)
                .config("spark.streaming.stopGracefullyOnShutdown", True)
                .config("spark.sql.shuffle.partitions", partitions)
                # .config(
                #     "spark.jars.packages",
                #     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
                # )
                # .config(
                #     "spark.sql.debug.maxToStringFields", "1000"
                # )
                # .config(
                #     "spark.sql.adaptive.enabled", "true"
                # )
                # .config(
                #     "spark.sql.debug.maxToStringFields", "1000"
                # )
                .config(
                "spark.sql.streaming.checkpointLocation",check_point_path
                )
                                
                .config("spark.driver.host", "airflow-airflow-worker-1"
                )
                .config("spark.driver.bindAddress", "0.0.0.0"
                )
                .config(
                        "spark.driver.port", "40001"
                )
                .config(
                    "spark.blockManager.port", "40002"
                )
                .getOrCreate()
            )
