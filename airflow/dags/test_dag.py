from airflow.sdk import dag, task
from datetime import datetime
from common.spark import make_spark_task
from common.defaults import default_args

@dag(
    dag_id="simple_example",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["example"],
)
def test_spark():
    make_spark_task(
        task_id = "test_spark_task", 
        application = "/opt/airflow/spark_jobs/test_spark.py", 
        app_args=None
    )



test_spark()