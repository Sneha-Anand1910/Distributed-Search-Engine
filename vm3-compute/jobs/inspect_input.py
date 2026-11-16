import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True)
parser.add_argument("--snapshot-id", required=True)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("VM3-Inspect-Input")
    .getOrCreate()
)

try:
    docs = (
        spark.read
        .format("iceberg")
        .option("snapshot-id", args.snapshot_id)
        .load(args.table)
    )

    docs.printSchema()
    docs.show(5, truncate=100)

    print("DOCUMENT_COUNT:", docs.count())
    print("SNAPSHOT_ID:", args.snapshot_id)

finally:
    spark.stop()
