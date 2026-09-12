import argparse
from datetime import datetime, timedelta
from spark_jobs.shared.spark_session import get_spark

def expire_snapshots(spark, 
                     table_name, 
                     layer="silver", 
                     retention_days=7):
    
    full_table = f"lake.{layer}.{table_name}"
    cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"Expiring snapshots older than {cutoff} for {full_table}...")

    result = spark.sql(f"""
        CALL lake.system.expire_snapshots(
            table => '{layer}.{table_name}',
            older_than => TIMESTAMP '{cutoff}'
        )
    """)
    result.show(truncate=False)
    print(f"Snapshot expiry complete for {full_table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--layer", default="silver")
    parser.add_argument("--retention-days", type=int, default=7)
    args = parser.parse_args()

    spark = get_spark(app_name=f"expire_snapshots_{args.table}")
    expire_snapshots(spark, args.table, args.layer, args.retention_days)
    spark.stop()