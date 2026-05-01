# Docker stack — local Glue execution

Components:

- **postgres** — Airflow metadata DB.
- **minio** — S3-compatible object store, listening on `:9000` (API) and `:9001` (UI).
- **minio-init** — one-shot container that creates the buckets and uploads the sample CSV.
- **glue-runner** — long-running container based on `public.ecr.aws/glue/aws-glue-libs:5`. Sleeps forever; Airflow `docker exec`s `spark-submit` into it per task. PySpark scripts are bind-mounted at `/scripts/`.
- **airflow-init** / **airflow-scheduler** / **airflow-webserver** — Airflow 2.9, with `glue-airflow-local` installed editably from `../glue-airflow-local`.

## How DAGs reach Airflow

The `airflow-dags` volume is shared between the scheduler and webserver. To run a generated DAG:

1. Run the translator with `--executor=glue-docker --output dags/<workflow>.py`.
2. Drop the generated file into the `airflow-dags` volume (e.g., copy or `docker cp`).
3. Trigger from the Airflow UI.

Future improvement: have the translator write directly into the volume by default.

## How S3 calls reach MinIO

The Glue runner has these env vars set (see `docker-compose.yaml` `glue-runner.environment`):

```
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
AWS_ENDPOINT_URL=http://minio:9000
AWS_ENDPOINT_URL_S3=http://minio:9000
```

Spark/Hadoop S3A configuration is in `spark-defaults.conf` baked into the Glue image (see `glue-runner.Dockerfile`). Both boto3 (>=1.30) and PySpark's `s3a://` filesystem honor these. User scripts call `s3://...` (or `s3a://...`) and don't know they're talking to MinIO.

## Bucket layout

The init container creates three buckets:

- `local-input` — sample input data (the seed CSV is uploaded here).
- `local-staging` — intermediate artifacts between jobs.
- `local-output` — final results.

These names are the defaults baked into `examples/simple-etl/default_params.json`.

## Security — local development only

This stack is **for local development only**. It contains:

- **Plaintext default credentials** for Airflow (`airflow` / `airflow`), MinIO (`minio` / `minio123`), and Postgres (`airflow` / `airflow`).
- **A bind-mount of the host's Docker socket** (`/var/run/docker.sock`) into the Airflow scheduler so the operator can `docker exec` into the Glue container. Anything running in that container has root-equivalent access on the host Docker daemon.

Do not expose any of these services on a network you don't fully control. Don't run this stack on a shared host. The Glue 5 image is pinned to a specific digest (see `glue-runner.Dockerfile`); bump intentionally when adopting a newer Glue runtime.
