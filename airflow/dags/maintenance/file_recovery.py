from airflow.sdk import dag, task
from datetime import datetime
from common.spark import make_mapped_spark_task
from common.defaults import default_args
from airflow.models.param import Param # type:ignore


SILVER_TABLES = [
    "orders", "payments", "order_items", "order_status_events",
    "reviews", "drivers", "menu_items",
    "customers", "restaurants", "cities", "zones",
]

@dag(
    dag_id="files_recovery",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["snapshots", "files"],
    # params={
    #     "inital_load": Param(False, type="boolean"),
    # }
    default_args = default_args,
    max_active_runs=1,
    max_active_tasks=2
)

def files_recovery():
    layer = 'silver'    
        
    
    compact_table = make_mapped_spark_task(
                task_id = f"compact_table", 
                application = "/opt/airflow/spark_jobs/maintenance/compact_table.py", 
            # bm_port ="40003",
            # dr_port ="40004",
            #pool_name="spark_cdc_pool"
            ).expand(
                application_args=[
                                      [  "--table", t,
                                        "--layer", layer]
                             for t in SILVER_TABLES
                        ]
                 )
    expire_snapshots = make_mapped_spark_task(
                    task_id = f"expire_snapshots", 
                    application = "/opt/airflow/spark_jobs/maintenance/expire_snapshots.py", 
                # bm_port ="40003",
                # dr_port ="40004",
               # pool_name="spark_cdc_pool"
                ).expand(
                application_args=[
                            [  "--table", t,
                            "--layer", layer]
                             for t in SILVER_TABLES
                        ]
                 )
    remove_orphans = make_mapped_spark_task(
                    task_id = f"remove_orphans", 
                    application = "/opt/airflow/spark_jobs/maintenance/remove_orphans.py", 
                # bm_port ="40003",
                # dr_port ="40004",
                #pool_name="spark_cdc_pool"
                ).expand(
                application_args=[
                            [  "--table", t,
                            "--layer", layer]
                             for t in SILVER_TABLES
                        ]
                 )
    compact_table >> expire_snapshots >> remove_orphans
files_recovery()