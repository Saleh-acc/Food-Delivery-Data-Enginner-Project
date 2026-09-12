from airflow.sdk import dag, task
from datetime import datetime
from common.spark import make_spark_task
from common.defaults import default_args
from airflow.models.param import Param # type:ignore
from airflow.sdk import task, get_current_context
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import boto3
from spark_jobs.shared.spark_session import  get_spark
from spark_jobs.shared.update_state import write_state, read_state, get_latest_state_path
import pyspark.sql.dataframe
from common.duckdb import get_duckdb_conn



@dag(
    dag_id="transform_data_bronze_silver_customers",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["silver","cdc","transform", "customers"],
    # params={
    #     "inital_load": Param(False, type="boolean"),
    # }
)

def transform_data_bronze_silver_customers():
    

    @task()
    def check_has_data():
        access_key = Variable.get("minio_access_key")
        secret_key = Variable.get("minio_secret_key")
        key_pre_cst = get_latest_state_path(table_name=table_name,
                                        minio_access_key= access_key,
                                        minio_secret_key= secret_key,
                                        prefix = "airflow/silver/pipeline-state/"
                                        ) or ''
        k = key_pre_cst+"customers_raw_df.parquet"
        print(f"trying to read from tasks: {k}")
        path = f"s3://lake/{k}"
        con = get_duckdb_conn()
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0] 
        print("number of fetched rows: ",n)
        if n == 0:
                 raise AirflowSkipException("the df_raw is empty. No updated data")
    
    table_name = "customers"
    bucket_path = "airflow/silver/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    
    customers_extract_data = make_spark_task(
                task_id = "customers_extract_data", 
                application = "/opt/airflow/spark_jobs/silver/scd3/customers.py", 
                app_args=[
                "--type",  "extract",
                "--prev_state_path",'None',
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    customers_transform_data = make_spark_task(
                task_id = "customers_transform_data", 
                application = "/opt/airflow/spark_jobs/silver/scd3/customers.py", 
                app_args=[
                "--type",  "transform",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    customers_load_data = make_spark_task(
                task_id = "customers_load_data", 
                application = "/opt/airflow/spark_jobs/silver/scd3/customers.py", 
                app_args=[
                "--type",  "load",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    is_df_not_empty = check_has_data()
    customers_extract_data >> is_df_not_empty >> customers_transform_data >> customers_load_data
        

@dag(
    dag_id="transform_data_bronze_silver_restaurants",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["silver","cdc","transform", "restaurants"],
    # params={
    #     "inital_load": Param(False, type="boolean"),
    # }
)

def transform_data_bronze_silver_restaurants():
    

    @task()
    def check_has_data():
        access_key = Variable.get("minio_access_key")
        secret_key = Variable.get("minio_secret_key")
        key_pre_cst = get_latest_state_path(table_name=table_name,
                                        minio_access_key= access_key,
                                        minio_secret_key= secret_key,
                                        prefix = "airflow/silver/pipeline-state/"
                                        ) or ''
        
        # df_raw = read_state(
        #         key=key_pre_cst+"restaurants_raw_data.parquet", 
        #         minio_access_key= access_key,
        #         minio_secret_key=secret_key,
        #         local_spark=True
        #     )
        k = key_pre_cst+"restaurants_raw_df.parquet"
        print(f"trying to read from tasks: {k}")
        path = f"s3://lake/{k}"
        con = get_duckdb_conn()
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0] 
        print("number of fetched rows: ",n)
        if n == 0:
                 raise AirflowSkipException("the df_raw is empty. No updated data")
    
    table_name = "restaurants"
    bucket_path = "airflow/silver/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    
    extract_data = make_spark_task(
                task_id = "resturants_extract_data", 
                application = "/opt/airflow/spark_jobs/silver/scd2/restaurants.py", 
                app_args=[
                "--type",  "extract",
                "--prev_state_path",'None',
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    transform_data = make_spark_task(
                task_id = "restaurants_transform_data", 
                application = "/opt/airflow/spark_jobs/silver/scd2/restaurants.py", 
                app_args=[
                "--type",  "transform",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    load_data = make_spark_task(
                task_id = "restaurants_load_data", 
                application = "/opt/airflow/spark_jobs/silver/scd2/restaurants.py", 
                app_args=[
                "--type",  "load",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    is_df_not_empty = check_has_data()
    extract_data >> is_df_not_empty >> transform_data >> load_data
        

@dag(
    dag_id="transform_data_bronze_silver_zones",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["silver","cdc","transform", "zones"],
    # params={
    #     "inital_load": Param(False, type="boolean"),
    # }
)

def transform_data_bronze_silver_zones():
    

    @task()
    def check_has_data():
        access_key = Variable.get("minio_access_key")
        secret_key = Variable.get("minio_secret_key")
        k = get_latest_state_path(table_name=table_name,
                                        minio_access_key= access_key,
                                        minio_secret_key= secret_key,
                                        prefix = "airflow/silver/pipeline-state/"
                                        ) or ''
        k = k + 'zones_raw_df.parquet'
        print(f"trying to read from tasks: {k}")
        path = f"s3://lake/{k}"
        con = get_duckdb_conn()
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0] 
        print("number of fetched rows: ",n)
        if n == 0:
                 raise AirflowSkipException("the df_raw is empty. No updated data")
    
    table_name = "zones"
    bucket_path = "airflow/silver/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    application = "/opt/airflow/spark_jobs/silver/scd0/zones.py"
    extract_data = make_spark_task(
                task_id = f"{table_name}_extract_data", 
                application = application, 
                app_args=[
                "--type",  "extract",
                "--prev_state_path",'None',
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    transform_data = make_spark_task(
                task_id = f"{table_name}_transform_data", 
                application = application, 
                app_args=[
                "--type",  "transform",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    load_data = make_spark_task(
                task_id = f"{table_name}_load_data", 
                application = application, 
                app_args=[
                "--type",  "load",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    is_df_not_empty = check_has_data()
    extract_data >> is_df_not_empty >> transform_data >> load_data
      
 
@dag(
    dag_id="transform_data_bronze_silver_cities",
    start_date=datetime(2025, 1, 1),
   # schedule="@daily",
    catchup=False,
    tags=["silver","cdc","transform", "cities"],
    # params={
    #     "inital_load": Param(False, type="boolean"),
    # }
)
def transform_data_bronze_silver_cities():
    

    @task()
    def check_has_data():
       
        access_key = Variable.get("minio_access_key")
        secret_key = Variable.get("minio_secret_key")
        key_pre_cst = get_latest_state_path(table_name=table_name,
                                        minio_access_key= access_key,
                                        minio_secret_key= secret_key,
                                        prefix = "airflow/silver/pipeline-state/"
                                        ) or ''
        
        k = key_pre_cst+"cities_raw_df.parquet"
        print(f"trying to read from tasks: {k}")
        path = f"s3://lake/{k}"
        con = get_duckdb_conn()
        n = con.execute(f"SELECT count(*) FROM read_parquet('{path}/**/*.parquet')").fetchone()[0] 
        print("number of fetched rows: ",n)
        if n == 0:
                 raise AirflowSkipException("the df_raw is empty. No updated data")

    table_name = "cities"
    bucket_path = "airflow/silver/pipeline-state/{{ ts_nodash  }}-"+ table_name.strip().replace(',', '-')
    application = "/opt/airflow/spark_jobs/silver/scd0/cities.py"
    extract_data = make_spark_task(
                task_id = f"{table_name}_extract_data", 
                application = application, 
                app_args=[
                "--type",  "extract",
                "--prev_state_path",'None',
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    transform_data = make_spark_task(
                task_id = f"{table_name}_transform_data", 
                application = application, 
                app_args=[
                "--type",  "transform",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    load_data = make_spark_task(
                task_id = f"{table_name}_load_data", 
                application = application, 
                app_args=[
                "--type",  "load",
                "--prev_state_path", bucket_path,
                "--current_state_path", bucket_path,
                
            ],
            bm_port ="40003",
            dr_port ="40004",
                
            )
    is_df_not_empty = check_has_data()
    extract_data >> is_df_not_empty >> transform_data >> load_data
     
transform_data_bronze_silver_customers()
transform_data_bronze_silver_restaurants()
transform_data_bronze_silver_zones()
transform_data_bronze_silver_cities()