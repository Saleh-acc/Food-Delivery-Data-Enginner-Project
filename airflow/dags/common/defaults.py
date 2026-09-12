from datetime import timedelta
from airflow import DAG
from datetime import datetime


# retry_delay: a timedelta of how long to wait between retries
# depends_on_past: a DAG run won't start unless the previous run succeeded
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False
}