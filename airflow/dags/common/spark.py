from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator #type: ignore
from datetime import timedelta

def make_spark_task(task_id:str, 
                    application:str, 
                    bm_port = "40001",
                    dr_port = "40002",
                    pool_name = None,
                    app_args=None, execution_timeout = None) -> SparkSubmitOperator:
    """
    task_id — the name that appears in the Airflow UI for this task
    application — path to the PySpark script, e.g. "/opt/airflow/spark_jobs/silver/orders.py"
    app_args — optional list of arguments passed to your script, default None
    """
    return SparkSubmitOperator(application = application,
                                task_id = task_id,
                                application_args = app_args,
                                verbose=False,
                                pool=pool_name,
                                execution_timeout=execution_timeout,
                                retries=1,
                                conf={
                                    # secrets via Jinja template — resolved at RUN time, not parse time
                                    "spark.hadoop.fs.s3a.access.key": "{{ var.value.minio_access_key }}",
                                    "spark.hadoop.fs.s3a.secret.key": "{{ var.value.minio_secret_key }}",
                                     "spark.authenticate": "true",
                                    "spark.authenticate.secret": "{{ var.value.spark_auth_secret }}",
                                    "spark.ssl.keyStorePassword": "{{ var.value.spark_ssl_password }}",
                                    "spark.ssl.keyPassword": "{{ var.value.spark_ssl_password }}",
                                        "spark.driver.host": "airflow-airflow-worker-1",  
                                        "spark.driver.bindAddress": "0.0.0.0",
                                     
                                         "spark.driver.port": dr_port,
                                        "spark.blockManager.port": bm_port,
                                        "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
                                        "spark.executor.memory":        "2g",
                                        "spark.executor.instances": "1",
                                        "spark.cores.max": "1",
                                        "spark.executor.memoryOverhead": "512m",
                                        "spark.executor.cores":         "1",
                                        "spark.driver.memory":          "1g",
                                        "spark.driver.memoryOverhead":  "256m",
                                        "spark.sql.shuffle.partitions": "1",
                                        "spark.setLogLevel": 'WARN'
                                         
}                                      
                                ,
                                env_vars={
                                            "PYTHONPATH": "/opt/airflow",
                                            "PYSPARK_PYTHON": "python3.12",
                                            "PYSPARK_DRIVER_PYTHON": "python3.12",
                                        }
                                )
    
    
    
def make_mapped_spark_task(task_id: str,
                            application: str,
                           
                            bm_port="40001",
                            dr_port="40002",
                            pool_name=None,
                            execution_timeout=None):
    """
    Same config as make_spark_task, but returns a PARTIAL operator
    for use with .expand(application_args=[...]) — application_args
    is intentionally NOT set here, since .expand() supplies it per-mapped-instance.
    """
    return SparkSubmitOperator.partial(
        application=application,
        task_id=task_id,
        verbose=False,
        pool=pool_name,
        execution_timeout=execution_timeout,
        retries=1,
        conf={
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.minio_access_key }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.minio_secret_key }}",
            "spark.authenticate": "true",
            "spark.authenticate.secret": "{{ var.value.spark_auth_secret }}",
            "spark.ssl.keyStorePassword": "{{ var.value.spark_ssl_password }}",
            "spark.ssl.keyPassword": "{{ var.value.spark_ssl_password }}",
            "spark.driver.host": "airflow-airflow-worker-1",
            "spark.driver.bindAddress": "0.0.0.0",
            # "spark.driver.port": dr_port,
            # "spark.blockManager.port": bm_port,
            "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
            "spark.executor.memory": "512m",
            "spark.executor.instances": "1",
            "spark.cores.max": "1",
            "spark.executor.memoryOverhead": "256m",
            "spark.executor.cores": "1",
            "spark.driver.memory": "512m",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "1",
            "spark.setLogLevel": "WARN",
        },
        env_vars={
            "PYTHONPATH": "/opt/airflow",
            "PYSPARK_PYTHON": "python3.12",
            "PYSPARK_DRIVER_PYTHON": "python3.12",
        },
    )