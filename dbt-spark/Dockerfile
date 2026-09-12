FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y \
    default-jdk-headless \
    procps \
    git \
    && apt-get clean

COPY --from=my-spark:3.5.3 /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$SPARK_HOME/bin:$PATH

RUN pip install --no-cache-dir \
    "dbt-spark[session]==1.10.0" \
    "pyspark==3.5.3"

ENV SPARK_CONF_DIR=/opt/dbt/conf
ENV DBT_PROFILES_DIR=/opt/dbt/profiles

# # Pre-warm the Ivy cache so dbt run never downloads jars at runtime
# RUN cd /tmp && python3.11 -c "
#             from pyspark.sql import SparkSession
#             SparkSession.builder \
#                 .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') \
#                 .master('local') \
#                 .getOrCreate() \
#                 .stop()
#             "
WORKDIR /opt/dbt