# glue-airflow-local

Translate AWS Glue Workflow Terraform definitions into Airflow DAGs that run on your laptop. Two execution modes:

- **Mock** (default, fast) — emit DAGs whose tasks are configurable mock operators (succeed / fail / sleep). Iterate on workflow shape without Spark or Docker.
- **Glue Docker** (real PySpark) — emit DAGs that `spark-submit` the same `*.py` job scripts that would run in cloud Glue, into a local `aws-glue-libs:5` container, with MinIO standing in for S3.

## Install

```bash
pip install -e ".[dev]"
```

## Usage

```bash
# Mock executor (default)
glue-airflow-local translate path/to/terraform/dir \
    --output dags/my_workflow.py \
    --workflow-dir path/to/workflow

# Glue Docker executor (requires the docker stack from ../docker/)
glue-airflow-local translate path/to/terraform/dir \
    --output dags/my_workflow.py \
    --workflow-dir path/to/workflow \
    --executor glue-docker
```

See the workspace-level [`../examples/simple-etl/`](../examples/simple-etl) for a runnable end-to-end example covering both modes, and [`../docker/`](../docker) for the local Glue + MinIO stack.

## Status

v0.3. Reads `aws_glue_workflow` + `aws_glue_trigger` (ON_DEMAND, CONDITIONAL, SCHEDULED) from a Terraform directory; emits Airflow DAGs that either run with mock operators or `docker exec` `spark-submit` into a long-running Glue 5 container. Generated DAGs are filesystem-independent — `default_params.json` is inlined at translate time.

See the workspace-level [`../README.md`](../README.md) for the full Known Limitations list and the v0.2 → v0.3 breaking-change note.
