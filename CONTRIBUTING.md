# Contributing

## Setup

```bash
cd glue-airflow-local
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Quality gate

Before opening a PR:

```bash
pytest                # all tests pass
ruff check src        # lint clean
mypy src              # type-clean under --strict
```

## Tests

- One assert per test where reasonable.
- Test names follow `test_<unit>__<scenario>__<expected>` or `test_<scenario>` for short.
- Fixtures in `tests/fixtures/` are real Terraform files; treat them as part of the public surface.

## Scope

v0.3 covers: Terraform input, jobs only, three trigger types (ON_DEMAND, CONDITIONAL, SCHEDULED), and two executors (mock + glue-docker). Out-of-scope: crawlers, EVENT triggers, job bookmarks, Glue Catalog API mocks. These are deferred to later plans — please open an issue before starting.

## Docker stack (Plan B)

The Glue Docker executor lives in [`docker/`](./docker). To run the end-to-end test:

```bash
docker compose -f docker/docker-compose.yaml up -d --build
cd glue-airflow-local
pytest -m docker_e2e -v
```

The `glue-runner` image is large (~5GB+); first build is slow (Glue 5 pull dominates). Subsequent builds reuse cached layers.

When developing the operator or translator, you don't need to rebuild — `glue-airflow-local` is mounted into the Airflow containers from the host and installed editably. Restart the scheduler to pick up code changes:

```bash
docker compose -f docker/docker-compose.yaml restart airflow-scheduler
```
