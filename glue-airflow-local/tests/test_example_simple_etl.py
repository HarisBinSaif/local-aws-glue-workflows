"""End-to-end smoke test for examples/simple-etl."""

from __future__ import annotations

from pathlib import Path

from glue_airflow_local.parser import parse_directory
from glue_airflow_local.translator import translate_workflow

EXAMPLE_DIR = Path(__file__).resolve().parents[2] / "examples" / "simple-etl"
# Fixed relative path used when generating expected_dag.py — keeps the golden
# file portable across machines / checkouts. Real CLI invocations pass whatever
# path the user supplies; the golden test isolates translator output from
# filesystem layout.
GOLDEN_WORKFLOW_DIR = "examples/simple-etl"


def test_simple_etl_generates_expected_dag():
    workflows = parse_directory(EXAMPLE_DIR / "terraform")
    assert [w.name for w in workflows] == ["simple-etl"]
    source = translate_workflow(workflows[0], workflow_dir=GOLDEN_WORKFLOW_DIR)
    expected = (EXAMPLE_DIR / "expected_dag.py").read_text()
    assert source == expected


def test_generated_dag_compiles():
    workflows = parse_directory(EXAMPLE_DIR / "terraform")
    source = translate_workflow(workflows[0], workflow_dir=GOLDEN_WORKFLOW_DIR)
    compile(source, "<example_dag>", "exec")
