# simple-etl example

A 3-job Glue Workflow (extract → transform → load) translated into an Airflow DAG.

## Generate the DAG

```bash
glue-airflow-local translate ./terraform \
    --output dags/simple_etl.py \
    --workflow-dir .
```

## What you should see

- A new file `dags/simple_etl.py` containing a `DAG(dag_id="simple-etl", ...)`.
- Three `MockGlueJobOperator` tasks: `extract_job`, `transform_job`, `load_job`.
- Dependencies: `extract_job >> transform_job >> load_job`.

## Run it locally

Drop `dags/simple_etl.py` into your Airflow `dags/` folder and trigger the DAG. With the default mock executor, each task succeeds instantly and prints the merged parameters.
