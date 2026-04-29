# AWS-Airflow workspace

Two deliverables:

1. [`glue-airflow-local/`](./glue-airflow-local) — Python tool that translates AWS Glue Workflow Terraform into runnable Airflow DAGs.
2. `article/` (forthcoming) — long-form article walking through the design.

See [`CLAUDE.md`](./CLAUDE.md) for the working agreements and scope.

## Quickstart

```bash
cd glue-airflow-local
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
glue-airflow-local translate ../examples/simple-etl/terraform \
    --output /tmp/simple_etl_dag.py \
    --workflow-dir ../examples/simple-etl
```

## Status

v0.1 — reads `aws_glue_workflow` + `aws_glue_trigger` (ON_DEMAND, CONDITIONAL, SCHEDULED) and emits Airflow DAGs that run with mock operators. No Docker yet — that's Plan B.

## License

Apache 2.0.
