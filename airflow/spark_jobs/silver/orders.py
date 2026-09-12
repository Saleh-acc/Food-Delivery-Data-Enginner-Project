from pyspark.sql import SparkSession 

try: 
    from pyspark import SparkContext 
    sc = SparkContext._active_spark_context
    if sc: 
        sc.stop()
        print(" Stoped previous SparkContext")
except:
    pass

spark = (
    SparkSession
    .builder
    .appName("Smoke test from airflow")
    .getOrCreate()
)
print(f"Event log dir: {spark.sparkContext._conf.get('spark.eventLog.dir', 'NOT SET')}")
print(f"Event log enabled: {spark.sparkContext._conf.get('spark.eventLog.enabled', 'NOT SET')}")
print("✓ Spark session started")
print(f"✓ Spark version: {spark.version}")

spark.stop()
print("✓ Spark session stopped")