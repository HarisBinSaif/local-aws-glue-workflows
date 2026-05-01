# local-aws-glue-workflows

Run AWS Glue Workflows on your laptop. Parse the Terraform you'd deploy to AWS, run it as a local Airflow DAG, and iterate without the cloud round-trip.

The Python package lives in [`glue-airflow-local/`](./glue-airflow-local). A runnable example is in [`examples/simple-etl/`](./examples/simple-etl). The local Docker stack lives in [`docker/`](./docker).

## Two execution modes

**Mock executor** (default, fast) — emit Airflow DAG that uses `MockGlueJobOperator`. Tasks succeed/fail/sleep on demand. Iterate on workflow shape (dependency graph, trigger semantics) without Spark or Docker.

**Glue Docker executor** (real PySpark) — same Terraform, same job scripts. The generated DAG calls `spark-submit` inside a long-running `aws-glue-libs:5` container; S3 calls hit MinIO via the boto3 endpoint override. The same `*.py` files would run unmodified in cloud Glue.

## Quickstart — mock executor

```bash
cd glue-airflow-local
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
glue-airflow-local translate ../examples/simple-etl/terraform \
    --output /tmp/simple_etl_dag.py \
    --workflow-dir ../examples/simple-etl
```

The generated `/tmp/simple_etl_dag.py` is a runnable Airflow DAG. Drop it into your Airflow `dags/` folder and trigger.

## Quickstart — real PySpark on the local stack

```bash
# 1. Bring up Airflow + Glue runner + MinIO (first build pulls Glue 5, ~5GB)
docker compose -f docker/docker-compose.yaml up -d --build

# 2. Generate the DAG with the glue-docker executor
glue-airflow-local translate examples/simple-etl/terraform \
    --output /tmp/simple_etl_dag.py \
    --workflow-dir examples/simple-etl \
    --executor glue-docker

# 3. Copy into the scheduler's dags volume, register, unpause, trigger with params
docker cp /tmp/simple_etl_dag.py \
    "$(docker compose -f docker/docker-compose.yaml ps -q airflow-scheduler)":/opt/airflow/dags/

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags reserialize

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags unpause simple-etl

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags trigger simple-etl
```

Airflow UI: http://localhost:8080 (airflow / airflow). Watch `simple-etl` run end-to-end (~30–60s after first Spark warm-up). When it succeeds, the result Parquet is at `local-output/department-summary/` in MinIO (UI: http://localhost:9001, login `minio` / `minio123`).

See [`examples/simple-etl/README.md`](./examples/simple-etl/README.md) for the full walkthrough.

## Status

v0.3 — both executor modes work end-to-end. Generated DAGs are filesystem-independent (`default_params` inlined at translate time). Glue 5.0 image pinned to a specific digest. Tested on Linux and Apple Silicon Docker.

## Known limitations

- **Glue Catalog calls are not supported.** `glueContext.create_dynamic_frame.from_catalog(...)` and similar fail. Use `from_options` with explicit S3 paths.
- **Crawlers are out of scope.** Jobs only.
- **Trigger types**: ON_DEMAND + CONDITIONAL + SCHEDULED. EVENT (EventBridge) triggers are rejected at parse time.
- **OR predicates and non-`SUCCEEDED` conditions are explicitly rejected** at translate time rather than silently mistranslated to AND/SUCCEEDED.
- **Terraform reference resolution** is limited to inter-resource references (`aws_glue_job.foo.name`). `var.x`, `local.y`, `module.z.foo`, `data.*.foo` are not resolved.
- **Glue version**: 5.0 only. Older Glue runtimes (4.0/3.0) would need a different image.

## Security note

The local Docker stack is for development only. It uses plaintext default credentials (`airflow`/`airflow`, `minio`/`minio123`) and bind-mounts the host's Docker socket into the Airflow scheduler so the operator can `docker exec` into the Glue container. Don't expose any of these services on a network you don't fully control. See [`docker/README.md`](./docker/README.md#security--local-development-only) for details.

## v0.2 → v0.3 breaking change

The operator constructor signature changed: `MockGlueJobOperator` and `GlueDockerOperator` no longer take `workflow_dir`. Both now take `default_params: dict | None`, which the translator inlines into the generated DAG at translate time. **Re-run `glue-airflow-local translate ...` against your Terraform to regenerate any v0.2 DAGs**; loading an old DAG file will fail with a `TypeError` for the missing `workflow_dir` kwarg.

## License

Apache 2.0.
