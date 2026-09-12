# cdc_streaming_dag.py

from airflow.sdk import dag, task
from common.spark import make_spark_task
from datetime import datetime, timedelta
from airflow.models.param import Param
from common.defaults import default_args
from airflow.sdk import get_current_context



DAILY_8_AM = "0 8 * * *"
RUN_8_HOURS_IN_SEC = 300


def get_duration_in_sec():
        c = get_current_context()
        params = c.get("params", {})
        return params.get("duration",RUN_8_HOURS_IN_SEC)
        
@dag(
    dag_id="cdc_bronze_streaming",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["bronze", "cdc", "streaming", "kafka"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_bronze_streaming():
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task(
        task_id="stream_cdc_to_bronze",
        application="/opt/airflow/spark_jobs/bronze/cdc/streaming_pipeline.py",
        pool_name = "default_pool",
        app_args=[
            "--topics", "reviews",
            "--duration", duration,   # run for 5 minutes then exit cleanly
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
 
cdc_bronze_streaming()