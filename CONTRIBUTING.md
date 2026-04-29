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

See [`CLAUDE.md`](./CLAUDE.md). v0.1 is intentionally narrow: jobs only, three trigger types, Terraform input, mock executor. Out-of-scope work (real Glue Docker, crawlers, EVENT triggers, bookmarks) is for later plans — please open an issue before starting.
