from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("orders_silver").getOrCreate()

print("✓ Spark session started")
print(f"✓ Spark version: {spark.version}")

spark.stop()
print("✓ Done")