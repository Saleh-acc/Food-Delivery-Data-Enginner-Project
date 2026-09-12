#!/bin/bash
set -e

DDL_DIR="/opt/airflow/spark_jobs/ddl"   # adjust to the actual container path found above

SPARK_CONF=(
  --master spark://spark-master:7077
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.lake.type=hive
  --conf spark.sql.catalog.lake.warehouse=s3a://lake/warehouse
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.defaultCatalog=lake
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
)

echo "Running bronze_tables.sql..."
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f "${DDL_DIR}/bronze_tables.sql"

echo "Running silver_tables.sql..."
/opt/spark/bin/spark-sql "${SPARK_CONF[@]}" -f "${DDL_DIR}/silver_tables.sql"

echo "DDL execution complete."


# docker exec spark-master bash -c "/opt/spark/bin/spark-sql --master spark://spark-master:7077   -f /opt/spark-apps/ddl/bronze_tables.sql"