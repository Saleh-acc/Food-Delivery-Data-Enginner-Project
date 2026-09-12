from airflow.models import Variable
import duckdb



def get_duckdb_conn():
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")
    con = duckdb.connect()
    print(type(con))
    con.execute("SET s3_endpoint='minio-lb:9000'; SET s3_use_ssl=false; SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id={access_key}; SET s3_secret_access_key={secret_key};")
    
    return con