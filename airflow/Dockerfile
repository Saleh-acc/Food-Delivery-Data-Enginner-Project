

# ---- Base: pinned Airflow + Python ----
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
# ---- Versions you align with YOUR environment ----
# SPARK_VERSION MUST match your standalone Spark cluster (major.minor at minimum).
# Override at build time:  docker build --build-arg SPARK_VERSION=3.5.3 ...
ARG SPARK_VERSION=3.5.3
ARG SPARK_PROVIDER_VERSION=6.1.0

# =========================================================
# 1. OS packages (need root). A JRE is required because
#    spark-submit launches a JVM; curl fetches the Spark client.
# =========================================================
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
      curl \
      procps \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Arch-independent JAVA_HOME (resolves the real java path on amd64 OR arm64)
RUN ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/current-java
ENV JAVA_HOME=/usr/lib/jvm/current-java

# =========================================================
# 2. Spark client distribution -> a deterministic spark-submit on PATH
# =========================================================
ENV SPARK_HOME=/opt/spark
RUN mkdir -p ${SPARK_HOME} \
 && curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
      -o /tmp/spark.tgz \
 && tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 \
 && rm /tmp/spark.tgz
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# RUN pip install --no-cache-dir \
#       "apache-airflow-providers-apache-spark==${SPARK_PROVIDER_VERSION}" \
#       "pyspark==${SPARK_VERSION}" \
#       "apache-airflow-providers-hashicorp"

# ENV SPARK_HOME=/opt/spark
#/home/airflow/.local/lib/python3.12/site-packages/pyspark
# ENV PATH="${SPARK_HOME}/bin:${PATH}"
# COPY config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
# ENV SPARK_CONF_DIR=/opt/spark/conf
RUN mkdir -p /opt/airflow/dbt/lakehouse_gold/target && \
    chmod -R 777 /opt/airflow/dbt/lakehouse_gold/target
# =========================================================
# 3. Python deps (NEVER as root). Provider + version-matched pyspark.
# =========================================================
# RUN pip uninstall pyspark -y || true \
#  && rm -rf /home/airflow/.local/lib/python3.12/site-packages/pyspark* \
#  && rm -rf /usr/local/lib/python3.12/site-packages/pyspark*
# ENV PYTHONNOUSERSITE=1
# ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/airflow/spark_jobs:/opt/airflow"
# USER root
# Remove any conflicting pyspark installations
# RUN rm -rf /home/airflow/.local/lib/python3.12/site-packages/pyspark* \
#      && rm -rf /home/airflow/.local/lib/python3.12/site-packages/pyspark_client*
USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow-providers-apache-spark==${SPARK_PROVIDER_VERSION}" \
      "apache-airflow-providers-hashicorp" \
      "pyspark==${SPARK_VERSION}" \
      "duckdb==1.5.4" \
      "astronomer-cosmos" \
      "dbt-spark[PyHive]==1.10.0"


# WHY install pyspark LAST and AFTER provider:
# The provider may pull PySpark 4.x as a dependency.
# Installing our pinned version last ensures it wins.
#RUN pip install --no-cache-dir --force-reinstall "pyspark==${SPARK_VERSION}"
