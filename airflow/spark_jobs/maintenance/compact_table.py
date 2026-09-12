import argparse
from spark_jobs.shared.spark_session import get_spark

def compact_table(spark, table_name, layer="silver"):
    """
    Compacts small data files into fewer, larger files.
    Safe to run repeatedly — Iceberg only rewrites files that
    actually benefit from compaction.
    """
    full_table = f"lake.{layer}.{table_name}"
    print(f"Compacting {full_table}...")

    result = spark.sql(f"""
        CALL lake.system.rewrite_data_files(
            table => '{layer}.{table_name}'
        )
    """)
    result.show(truncate=False)
    print(f"Compaction complete for {full_table}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--layer", default="silver")
    args = parser.parse_args()

    spark = get_spark(app_name=f"compact_{args.table}")
    compact_table(spark, args.table, args.layer)
    spark.stop()