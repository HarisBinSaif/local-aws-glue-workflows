# glue-airflow-local

Translate AWS Glue Workflow Terraform definitions into Airflow DAGs that run on your laptop with mocks. No AWS calls, no Spark, no Docker (yet).

## Status

v0.1 — early. Reads `aws_glue_workflow` + `aws_glue_trigger` (ON_DEMAND, CONDITIONAL, SCHEDULED) from a Terraform directory and emits an Airflow DAG file whose tasks are mock Glue job operators.

## Install

```bash
pip install -e ".[dev]"
```

## Usage

```bash
glue-airflow-local translate path/to/terraform/dir --output dags/my_workflow.py
```

See `examples/simple-etl/` for a runnable end-to-end example.
