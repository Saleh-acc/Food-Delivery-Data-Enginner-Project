from airflow.sdk import dag, task
from datetime import datetime
from common.spark import make_spark_task
from common.defaults import default_args
from airflow.models.param import Param # type:ignore
from airflow.sdk import task, get_current_context

import json
def get_load_types():
        c = get_current_context()
        params = c.get("params", {})
        is_inital_load = params.get("inital_load")
        print("is_inital_load:",is_inital_load)
        if is_inital_load:
            return "inital_load"
        else:
            return "incremntial_load"
# config = {
#     "table": "customers",
#     "mode": "incremental",
#     "batch_size": 1000
# }
@dag(
    dag_id="ingest_source_bronze_customers",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["bronze","batches", "customers"],
    params={
        "inital_load": Param(False, type="boolean"),
    }
)

def ingest_bronze_silver_customers():
    table_name = "customers"
    bucket_path = "airflow/bronze/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    
    @task()
    def _get_load_types():
        return get_load_types()
        
        
    load_type = _get_load_types()
    customers_meta_data = make_spark_task(
            task_id = "get_meta_data_customers", 
            application = "/opt/airflow/spark_jobs/bronze/batch/get_metadata_tables.py", 
            app_args=[
            "--config",  "customers", "orders", "payments",
            "--load_type", load_type,
            "--is_initial_load_with_cdc", "False",
            "--current_state_path", bucket_path + "/meta_data.json",
            
        ],
        bm_port ="40003",
        dr_port ="40004",
            
        )
    
    customers_generate_batches = make_spark_task(
            task_id = "generate_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/l.py", 
            app_args=[
            "--config", table_name,
             "--prev_state_path",  "meta_data.json",
            "--current_state_path", bucket_path + "/generate_batches.json"
        ],
            
        )
    customers_insert_batches = make_spark_task(
            task_id = "insert_new_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/insert_new_batches.py", 
            app_args=[
            "--config", table_name,
            "--load_type", load_type,
            "--prev_state_path",  "generate_batches.json",
            "--current_state_path", bucket_path + "/insert_new_batches.json",
            "--tolerate_with_duplicate", "False",
        ],
            
        )
    
    
    customers_process_batches = make_spark_task(
            task_id = "process_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/process_batches.py", 
            app_args=[
            "--config", table_name,
            "--prev_state_path",  "generate_batches.json",
        ],
            
        )
    customers_meta_data >> customers_generate_batches >> customers_insert_batches >> customers_process_batches
    # customers_ingest = make_spark_task(
    #     task_id = "extract_customers", 
    #     application = "/opt/airflow/spark_jobs/bronze/batch/pipeline.py", 
    #     app_args=[
    #     "--config", "customers"
    # ]
    # )
    
   
    # zones_ingest = make_spark_task(
    #     task_id = "extract_zones", 
    #     application = "/opt/airflow/spark_jobs/bronze/batch/pipeline.py", 
    #     app_args=[
    #     "--config", "zones"
    # ]
    # )
    # [customers_ingest  ,zones_ingest]
    
    
    
@dag(
    dag_id="ingest_source_bronze_orders",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["bronze", "batches", "orders"],
    params={
        "inital_load": Param(False, type="boolean"),
    }
)
def ingest_bronze_silver_orders():
    table_name = "orders"
    bucket_path = "airflow/bronze/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    @task()
    def _get_load_types():
        return get_load_types()
        
        
    load_type = _get_load_types()
    meta_data = make_spark_task(
            task_id = f"get_meta_data_{table_name}", 
            application = "/opt/airflow/spark_jobs/bronze/batch/get_metadata_tables.py", 
            app_args=[
            "--config", table_name,
            "--load_type", load_type,
            "--is_initial_load_with_cdc", "False",
            "--current_state_path", bucket_path + "/meta_data.json"
        ],
            
        )
    generate_batches = make_spark_task(
            task_id = "generate_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/generate_batches.py", 
            app_args=[
            "--config", table_name,
             "--prev_state_path",  "meta_data.json",
            "--current_state_path", bucket_path + "/generate_batches.json"
        ],
            
        )
    insert_batches = make_spark_task(
            task_id = "insert_new_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/insert_new_batches.py", 
            app_args=[
            "--config", table_name,
            "--load_type", load_type,
            "--prev_state_path",  "generate_batches.json",
            "--current_state_path", bucket_path + "/insert_new_batches.json",
            "--tolerate_with_duplicate", "False",
        ],
            
        )
    
    
    process_batches = make_spark_task(
            task_id = "preprocess_batches_and_load_data", 
            application = "/opt/airflow/spark_jobs/bronze/batch/process_batches.py", 
            app_args=[
            "--config", table_name,
            "--prev_state_path",  "generate_batches.json",
        ],
            
        )
    meta_data >> generate_batches >> insert_batches >> process_batches



@dag(
    dag_id="ingest_source_bronze_all_tables",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["bronze", 
          "inital_load",
          "batches", 
          "customers", 
          "payments",
          "orders",
          "zones",
          "cities",
          "restaurants",
          "order_items",
          "order_status_events",
          "reviews",
          "drivers",
          "menu_items"
          ],
    
)
def ingest_bronze_silver_all_tables():
    table_name = "all_tables"
    bucket_path = "airflow/bronze/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    inital_load = "inital_load"
    
    meta_data = make_spark_task(
            task_id = "get_meta_data_customers", 
            application = "/opt/airflow/spark_jobs/bronze/batch/get_metadata_tables.py", 
            app_args=[
            "--config", "customers", 
                        "payments",
                        "orders",
                        "zones",
                        "cities",
                        "restaurants",
                        "order_items",
                        "order_status_events",
                        "reviews",
                       "drivers",
                       "menu_items",
            "--load_type", inital_load,
            "--is_initial_load_with_cdc", "False",
            "--current_state_path", bucket_path + "/meta_data.json",
            
        ],
            
        )
    generate_batches = make_spark_task(
            task_id = "generate_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/generate_batches.py", 
            app_args=[
            "--config", table_name,
             "--prev_state_path",  "meta_data.json",
            "--current_state_path", bucket_path + "/generate_batches.json"
        ],
            
        )
    insert_batches = make_spark_task(
            task_id = "insert_new_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/insert_new_batches.py", 
            app_args=[
            "--config", table_name,
            "--load_type", inital_load,
            "--prev_state_path",  "generate_batches.json",
            "--current_state_path", bucket_path + "/insert_new_batches.json",
            "--tolerate_with_duplicate", "False",
        ],
            
        )
    
    
    process_batches = make_spark_task(
            task_id = "process_batches", 
            application = "/opt/airflow/spark_jobs/bronze/batch/process_batches.py", 
            app_args=[
            "--config", table_name,
            "--prev_state_path",  "generate_batches.json",
        ],
            
        )
    meta_data >> generate_batches >> insert_batches >> process_batches

    
    

ingest_bronze_silver_customers()
ingest_bronze_silver_orders()
ingest_bronze_silver_all_tables()