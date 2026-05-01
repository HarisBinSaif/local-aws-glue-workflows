# simple-etl example

Three-job linear pipeline: `extract → transform → load`. The same Terraform deploys to AWS Glue or runs locally; the same PySpark scripts execute in either environment.

## Layout

- `terraform/main.tf` — `aws_glue_workflow` + jobs + triggers. Deploy this with `terraform apply` to run on AWS, or feed it to `glue-airflow-local translate` for local execution.
- `scripts/extract.py`, `transform.py`, `load.py` — the actual PySpark. Same files run in both environments.
- `default_params.json` — bucket names and other parameters merged into each job's `getResolvedOptions(...)` call.
- `sample-input.csv` — seed data for the local stack.
- `expected_dag.py` — golden reference DAG; the mock-mode translator is regression-tested against this file.

## Run it locally — mock executor (no Spark)

```bash
cd ../../glue-airflow-local
source .venv/bin/activate
glue-airflow-local translate ../examples/simple-etl/terraform \
    --output /tmp/simple_etl_dag.py \
    --workflow-dir ../examples/simple-etl
```

Drop `/tmp/simple_etl_dag.py` into your Airflow `dags/` folder and trigger. Each task succeeds instantly; you can verify the dependency graph and parameter merging without running PySpark.

## Run it locally — real PySpark in Docker

From the repo root:

```bash
# 1. Bring up the stack (Airflow + Glue 5 + MinIO + bucket init)
docker compose -f docker/docker-compose.yaml up -d --build

# 2. Generate the DAG
glue-airflow-local translate examples/simple-etl/terraform \
    --output /tmp/simple_etl_dag.py \
    --workflow-dir examples/simple-etl \
    --executor glue-docker

# 3. Hand it to Airflow + trigger with the params
docker cp /tmp/simple_etl_dag.py \
    "$(docker compose -f docker/docker-compose.yaml ps -q airflow-scheduler)":/opt/airflow/dags/

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags reserialize

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags unpause simple-etl

docker compose -f docker/docker-compose.yaml exec -T airflow-scheduler \
    airflow dags trigger simple-etl
```

The `default_params.json` is inlined into the generated DAG at translate time, so `airflow dags trigger simple-etl` runs with those defaults out of the box. To override at runtime, pass JSON via `--conf`, e.g. `airflow dags trigger simple-etl --conf '{"ENV":"staging"}'`.

Airflow UI: http://localhost:8080 (airflow / airflow). Trigger `simple-etl` and watch it run.

After the run completes (~30–60s after first Spark warm-up), check MinIO at http://localhost:9001 (minio / minio123) — the result Parquet is at `local-output/department-summary/`.

To tear the stack down (and wipe the volumes):

```bash
docker compose -f docker/docker-compose.yaml down -v
```

## Deploy to AWS

Same Terraform, no changes:

```bash
cd terraform
terraform init
terraform apply
```

Then trigger the `simple-etl` workflow in the AWS Glue console. Same scripts; same dependency graph; same `getResolvedOptions(...)` parameters.
