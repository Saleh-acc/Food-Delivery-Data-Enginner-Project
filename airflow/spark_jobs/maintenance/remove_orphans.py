import argparse
from spark_jobs.shared.spark_session import get_spark

def remove_orphan_files(spark, table_name, layer="silver"):
    full_table = f"lake.{layer}.{table_name}"
    print(f"Removing orphan files for {full_table}...")

    result = spark.sql(f"""
        CALL lake.system.remove_orphan_files(
            table => '{layer}.{table_name}'
        )
    """)
    result.show(truncate=False)
    print(f"Orphan cleanup complete for {full_table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--layer", default="silver")
    args = parser.parse_args()

    spark = get_spark(app_name=f"remove_orphans_{args.table}")
    remove_orphan_files(spark, args.table, args.layer)
    spark.stop()