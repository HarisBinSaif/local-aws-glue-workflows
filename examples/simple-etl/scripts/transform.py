"""Read extract output Parquet, aggregate salary by department, write back.

Local: hits MinIO via AWS_ENDPOINT_URL. Cloud: hits real S3.
"""

import sys

from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "STAGING_BUCKET"])
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    extract_path = f"s3a://{args['STAGING_BUCKET']}/extract/output"
    transform_path = f"s3a://{args['STAGING_BUCKET']}/transform/output"

    df = spark.read.parquet(extract_path)
    aggregated = (
        df.groupBy("department")
        .agg(
            F.count("*").alias("headcount"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
        )
        .orderBy("department")
    )
    aggregated.write.mode("overwrite").parquet(transform_path)

    spark.stop()


if __name__ == "__main__":
    main()
