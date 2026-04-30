"""Read sample-input.csv from MinIO and write it as Parquet to the staging path.

Runs in: AWS Glue (cloud), or aws-glue-libs Docker container (local).
The only difference between the two is the boto3 endpoint, which is set via
AWS_ENDPOINT_URL on the local container - no code change here.
"""

import sys

from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]
from pyspark.sql import SparkSession


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "INPUT_BUCKET", "INPUT_KEY", "STAGING_BUCKET"],
    )
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    input_path = f"s3a://{args['INPUT_BUCKET']}/{args['INPUT_KEY']}"
    staging_path = f"s3a://{args['STAGING_BUCKET']}/extract/output"

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    df.write.mode("overwrite").parquet(staging_path)

    spark.stop()


if __name__ == "__main__":
    main()
