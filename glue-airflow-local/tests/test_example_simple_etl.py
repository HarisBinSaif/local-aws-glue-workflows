"""End-to-end smoke test for examples/simple-etl."""

from __future__ import annotations

import json
from pathlib import Path

from glue_airflow_local.parser import parse_directory
from glue_airflow_local.translator import translate_workflow

EXAMPLE_DIR = Path(__file__).resolve().parents[2] / "examples" / "simple-etl"


def _load_default_params() -> dict:
    return json.loads((EXAMPLE_DIR / "default_params.json").read_text())


def test_simple_etl_generates_expected_dag():
    workflows = parse_directory(EXAMPLE_DIR / "terraform")
    assert [w.name for w in workflows] == ["simple-etl"]
    source = translate_workflow(
        workflows[0], default_params=_load_default_params()
    )
    expected = (EXAMPLE_DIR / "expected_dag.py").read_text()
    assert source == expected


def test_generated_dag_compiles():
    workflows = parse_directory(EXAMPLE_DIR / "terraform")
    source = translate_workflow(
        workflows[0], default_params=_load_default_params()
    )
    compile(source, "<example_dag>", "exec")
