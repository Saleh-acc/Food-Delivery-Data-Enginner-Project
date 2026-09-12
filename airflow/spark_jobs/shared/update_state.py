import json
import boto3
import re
import pyspark.sql.dataframe
from spark_jobs.shared.spark_session import get_spark

def write_state(key: str,minio_access_key:str,minio_secret_key:str, data):
    """Write pipeline state to MinIO as plain JSON"""
    print(f"the key:{key}")
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-lb:9000",
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    if isinstance(data, pyspark.sql.DataFrame):
        # write DataFrame as parquet to MinIO
        data.write.mode("overwrite").parquet(
            f"s3a://lake/{key}"
        )
        
    elif data is None:
        s3.put_object(
            Bucket="lake",
            Key=key,
            Body=json.dumps({"empty": True})
        )
    else:
        s3.put_object(
            Bucket="lake",
            Key=key,
            Body=json.dumps(data)
        )

    print(f"The data are stored succssfully in {key}")
def read_state(key: str,minio_access_key:str,minio_secret_key:str, local_spark = False) :
    """Read pipeline state from MinIO as plain JSON"""
    print(f"trying to read  {key}")
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-lb:9000",
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    

    
    try:

        spark = get_spark(app_name="read state")
        # try parquet first (DataFrame state)
        df = spark.read.parquet(f"s3a://lake/{key}")
        print(df)
        return df
    except Exception:
        pass
    
    try:
        response = s3.get_object(
            Bucket="lake",
            Key=key
        )
        data = json.loads(response["Body"].read())
        if data.get("empty"):
            return None
        return data
    except Exception:
        return None
    
    



def get_latest_state_path(table_name: str,minio_access_key:str,minio_secret_key:str, bucket: str = "lake",prefix = "airflow/bronze/pipeline-state/"):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio-lb:9000",
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
        
    )

    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    all_prefixes = response.get("CommonPrefixes", [])
    print(all_prefixes)
    print(all_prefixes[0]["Prefix"].rstrip("/").endswith(f"-{table_name}"))
    print(table_name)
    if not all_prefixes:
        return None

    # each prefix looks like: airflow/bronze/pipeline-state/20260710T021645-customers/
    matching = [
        re.search(r"(\d{8}T\d{6})", p["Prefix"]).group(0) for p in all_prefixes
        if p["Prefix"].rstrip("/").endswith(f"-{table_name.strip()}")
    ]
    print("matching",matching)
    if not matching:
        return None
    
    # ts_nodash format sorts correctly as plain strings (YYYYMMDDTHHMMSS)
    latest_date = max(matching)
    latest_path = [i["Prefix"] for i  in all_prefixes if latest_date in i["Prefix"]][0]
    print("latest_path ",latest_path)
    
    return latest_path