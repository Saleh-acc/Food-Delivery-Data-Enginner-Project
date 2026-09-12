# dags/gold/gold_dag.py

from airflow import DAG
from docker.types import Mount
from datetime import datetime, timedelta
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

DBT_PROJECT_PATH = "/opt/airflow/dbt/lakehouse_gold"
DBT_PROFILES_PATH = "/opt/airflow/dbt/profiles"

profile_config = ProfileConfig(
    profile_name="lakehouse_gold",           # matches dbt_project.yml
    target_name="dev",                        # matches your profiles.yml target
    profiles_yml_filepath=f"{DBT_PROFILES_PATH}/profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,       # run dbt in the Airflow env directly
)

gold_dag = DbtDag(
    dag_id="gold_cosmos_dag",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=execution_config,
    schedule=None,#"*/5 * * * *",                   # every 5 min (start conservative)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt", "cosmos"],
)