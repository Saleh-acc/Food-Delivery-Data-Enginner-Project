#!/bin/bash
# set -e

echo "Starting Spark master..."
/opt/spark/sbin/start-master.sh

echo "Waiting for Spark master to be ready..."
until curl -s http://localhost:8080 > /dev/null; do
  sleep 2
done

echo "Starting Thrift Server..."
/opt/spark/sbin/start-thriftserver.sh \
    --master spark://spark-master:7077 \
    --hiveconf hive.server2.thrift.port=10000 \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    --conf spark.driver.host=spark-master \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.cores.max=1 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=1g \
    --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.lake.type=hive \
    --conf spark.sql.catalog.lake.uri=thrift://hive-metastore:9083 \
    --hiveconf hive.metastore.uris=thrift://hive-metastore:9083 \
    --conf spark.sql.catalog.lake.warehouse=s3a://lake/warehouse \
    --conf spark.sql.defaultCatalog=lake \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar

echo "Waiting for Thrift Server to be ready..."
until nc -z localhost 10000; do
  sleep 2
done

# echo "Running DDL scripts..."
# /opt/spark/sbin/create_ddl.sh

echo "Startup complete — container staying alive."
echo "Startup complete — tailing master and thrift logs..."
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master.Master-1-spark-master.out \
        /opt/spark/logs/spark--org.apache.spark.sql.hive.thriftserver.HiveThriftServer2-1-spark-master.out