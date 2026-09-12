# cdc_streaming_dag.py

from airflow.sdk import dag, task
from common.spark import make_spark_task
from datetime import datetime, timedelta
from airflow.models.param import Param # type:ignore
from common.defaults import default_args
from airflow.sdk import get_current_context



DAILY_8_AM = "0 8 * * *"
RUN_8_HOURS_IN_SEC = 300


def get_duration_in_sec():
        c = get_current_context()
        params = c.get("params", {})
        return params.get("duration",RUN_8_HOURS_IN_SEC)
        
@dag(
    dag_id="cdc_silver_streaming_orders",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_orders():
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application="/opt/airflow/spark_jobs/silver/accumulating/orders.py",
        pool_name = "default_pool",
        app_args=[
            "--table", "orders",
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", "s3a://lake/spark/checkpoints/silver_writer/orders"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
 
 
        
@dag(
    dag_id="cdc_silver_streaming_payments",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "payments"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_payments():
    table_name = 'payments'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/accumulating/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )

        
@dag(
    dag_id="cdc_silver_streaming_order_items",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "order_items"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_order_items():
    table_name = 'order_items'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/append/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
 
 
@dag(
    dag_id="cdc_silver_streaming_order_status_events",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "order_status_events"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_order_status_events():
    table_name = 'order_status_events'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/append/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )

@dag(
    dag_id="cdc_silver_streaming_reviews",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "reviews"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_reviews():
    table_name = 'reviews'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/append/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
    
    
@dag(
    dag_id="cdc_silver_streaming_drivers",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "drivers"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_drivers():
    table_name = 'drivers'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/scd3/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
    
    
@dag(
    dag_id="cdc_silver_streaming_menu_items",
    # @continuous — restart immediately after previous run finishes.
    # Combined with max_active_runs=1, this creates a controlled
    # infinite loop. Pause the DAG from the UI to stop it cleanly.
    schedule=DAILY_8_AM,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      
    is_paused_upon_creation=True,
    tags=["silver", "cdc", "streaming", "iceberg", "menu_items"],
    default_args=default_args,
    params={
        "duration": Param(RUN_8_HOURS_IN_SEC, type="integer"),
    }
)
def cdc_silver_streaming_menu_items():
    table_name = 'menu_items'
    @task()
    def _get_duration_in_sec() -> str:
        return str(get_duration_in_sec())
        
    duration = _get_duration_in_sec() 
    print(timedelta(seconds=RUN_8_HOURS_IN_SEC + 120))
    stream_cdc = make_spark_task( 
        task_id="stream_cdc_to_bronze",
        application=f"/opt/airflow/spark_jobs/silver/scd2/{table_name}.py",
        pool_name = "default_pool",
        app_args=[
            "--table", table_name,
            "--duration", duration,   # run for 5 minutes then exit cleanly
            "--trigger_interval", "40 seconds",
            "--checkpoints", f"s3a://lake/spark/checkpoints/silver_writer/{table_name}"
        ],
        bm_port ="40001",
        dr_port ="40002",

       # pool="spark_cdc_pool",
        # execution_timeout slightly longer than --duration
        # so Airflow doesn't kill the job before it exits cleanly
       
        execution_timeout=timedelta(seconds=RUN_8_HOURS_IN_SEC + 120)
    )
    
cdc_silver_streaming_menu_items()
cdc_silver_streaming_drivers()
cdc_silver_streaming_order_status_events()
cdc_silver_streaming_order_items() 
cdc_silver_streaming_payments() 
cdc_silver_streaming_orders()