#!/bin/bash
set -e

# Start Spark Master
/opt/spark/sbin/start-master.sh

# Wait for the master to be ready
sleep 10

# Start Hive Thrift Server
/opt/spark/sbin/start-thriftserver.sh \
  --master spark://spark-master:7077

# Keep the container alive
tail -f /opt/spark/logs/*