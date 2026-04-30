# Long-running Glue 5.0 container. Sleeps forever so Airflow can `docker exec`
# `spark-submit` against it on demand. The user's PySpark scripts live on the host
# and are bind-mounted at /scripts.
FROM public.ecr.aws/glue/aws-glue-libs:5

USER root

# Glue 5 ships a non-trivial spark-defaults.conf (driver/executor classpaths,
# event log dir, network crypto, etc.). Append our S3A/MinIO settings rather
# than overwriting — losing the existing entries breaks spark-submit.
# /usr/lib/spark/conf is a symlink to /etc/spark/conf in the upstream image.
COPY docker/spark-defaults.conf /tmp/s3a-defaults.conf
RUN cat /tmp/s3a-defaults.conf >> /etc/spark/conf/spark-defaults.conf \
    && rm /tmp/s3a-defaults.conf \
    && chown hadoop:hadoop /etc/spark/conf/spark-defaults.conf

USER hadoop

# boto3 also honors AWS_ENDPOINT_URL — set in docker-compose.yaml so it can be
# overridden without rebuilding the image.
ENV AWS_DEFAULT_REGION=us-east-1

# The upstream image's ENTRYPOINT is `bash -l`, which would treat our CMD args
# as a script name. Reset it so the container just runs `sleep infinity`.
ENTRYPOINT []
CMD ["sleep", "infinity"]
