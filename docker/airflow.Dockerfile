# Airflow image with glue-airflow-local + Docker SDK installed.
# The package is installed editably from a bind-mount so iterating on
# the translator/operator does not require an image rebuild.
FROM apache/airflow:2.9.3-python3.11

USER airflow

# pip layer is split so Docker caches the public deps separately from
# the bind-mounted package install (which happens at container start).
RUN pip install --no-cache-dir 'docker>=7,<8'

# The local glue-airflow-local source is mounted at /opt/glue-airflow-local
# (see docker-compose.yaml). Install editably on first start via the
# entrypoint so live edits propagate.
ENV PYTHONPATH=/opt/glue-airflow-local/src:$PYTHONPATH
