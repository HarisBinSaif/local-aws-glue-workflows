"""Read transform output, write final result to the output bucket as a single Parquet file."""

import sys

from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.sql import SparkSession


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "STAGING_BUCKET", "OUTPUT_BUCKET"],
    )
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    transform_path = f"s3a://{args['STAGING_BUCKET']}/transform/output"
    final_path = f"s3a://{args['OUTPUT_BUCKET']}/department-summary"

    df = spark.read.parquet(transform_path)
    df.coalesce(1).write.mode("overwrite").parquet(final_path)

    spark.stop()


if __name__ == "__main__":
    main()
