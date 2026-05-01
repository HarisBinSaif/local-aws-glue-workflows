"""Tests for IR -> Airflow DAG translation."""

from __future__ import annotations

import ast
import keyword as _kw

import pytest

from glue_airflow_local.exceptions import UnsupportedTriggerError
from glue_airflow_local.model import (
    Action,
    Condition,
    Job,
    JobState,
    LogicalOperator,
    Predicate,
    Trigger,
    TriggerType,
    Workflow,
)
from glue_airflow_local.parser import parse_directory
from glue_airflow_local.translator import _identifier, translate_workflow


def _imports_mock_operator(source: str) -> bool:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if (
                node.module == "glue_airflow_local.operators"
                and any(alias.name == "MockGlueJobOperator" for alias in node.names)
            ):
                return True
    return False


def test_linear_chain_emits_runnable_dag(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "linear_chain")[0]
    source = translate_workflow(wf, default_params={})
    assert _imports_mock_operator(source)
    assert 'dag_id="linear-etl"' in source
    # Each job becomes a task
    for job in ("extract-job", "transform-job", "load-job"):
        assert f'job_name="{job}"' in source
    # Linear dependencies expressed via >>
    assert 'extract_job >> transform_job' in source or 'extract_job.set_downstream' in source
    # Source must compile
    compile(source, "<dag>", "exec")


def test_scheduled_dag_carries_cron(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "scheduled")[0]
    source = translate_workflow(wf, default_params={})
    # Cron expression preserved verbatim
    assert "cron(0 2 * * ? *)" in source


def test_parallel_branches_dag(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "parallel_branches")[0]
    source = translate_workflow(wf, default_params={})
    compile(source, "<dag>", "exec")
    # branch_a and branch_b both depend on src; merge depends on both
    assert "source_job" in source
    assert "branch_a_job" in source
    assert "branch_b_job" in source
    assert "merge_job" in source


def test_identifier_avoids_python_keywords():
    """A Glue job named after a Python keyword must not collide with it in the generated source."""
    assert _identifier("class") != "class"
    assert _kw.iskeyword(_identifier("class")) is False


def test_identifier_disambiguates_collisions_in_translate_workflow(tmp_path):
    """Two jobs that collapse to the same identifier each get a unique generated variable."""
    wf = Workflow(
        name="collide",
        triggers=[
            Trigger(
                name="start",
                type=TriggerType.ON_DEMAND,
                actions=[Action(job_name="foo-bar"), Action(job_name="foo_bar")],
            )
        ],
        jobs={
            "foo-bar": Job(name="foo-bar", script_location="s3://x/foo-bar.py"),
            "foo_bar": Job(name="foo_bar", script_location="s3://x/foo_bar.py"),
        },
    )
    source = translate_workflow(wf, default_params={})
    # Both jobs must appear as task_id values
    assert 'job_name="foo-bar"' in source
    assert 'job_name="foo_bar"' in source
    # The generated source must compile (no duplicate variable assignment overwriting an upstream)
    compile(source, "<dag>", "exec")
    # Two distinct variable names — count the assignments
    assert source.count(" = MockGlueJobOperator(") == 2


def test_or_predicate_rejected_at_translate(tmp_path):
    wf = Workflow(
        name="or-wf",
        triggers=[
            Trigger(
                name="start",
                type=TriggerType.ON_DEMAND,
                actions=[Action(job_name="a")],
            ),
            Trigger(
                name="any",
                type=TriggerType.CONDITIONAL,
                actions=[Action(job_name="b")],
                predicate=Predicate(
                    conditions=(
                        Condition(job_name="a"),
                        Condition(job_name="c"),
                    ),
                    logical=LogicalOperator.OR,
                ),
            ),
        ],
        jobs={
            "a": Job(name="a", script_location="s3://x/a.py"),
            "b": Job(name="b", script_location="s3://x/b.py"),
            "c": Job(name="c", script_location="s3://x/c.py"),
        },
    )
    with pytest.raises(UnsupportedTriggerError, match="OR predicate"):
        translate_workflow(wf, default_params={})


def test_failed_state_rejected_at_translate(tmp_path):
    wf = Workflow(
        name="failed-wf",
        triggers=[
            Trigger(
                name="start",
                type=TriggerType.ON_DEMAND,
                actions=[Action(job_name="a")],
            ),
            Trigger(
                name="cleanup",
                type=TriggerType.CONDITIONAL,
                actions=[Action(job_name="b")],
                predicate=Predicate(
                    conditions=(Condition(job_name="a", state=JobState.FAILED),),
                ),
            ),
        ],
        jobs={
            "a": Job(name="a", script_location="s3://x/a.py"),
            "b": Job(name="b", script_location="s3://x/b.py"),
        },
    )
    with pytest.raises(UnsupportedTriggerError, match=r"state 'FAILED'"):
        translate_workflow(wf, default_params={})


def test_translate_workflow_default_executor_is_mock(fixtures_dir, tmp_path):
    """Backwards-compat: no executor arg -> MockGlueJobOperator (the default)."""
    wf = parse_directory(fixtures_dir / "linear_chain")[0]
    source = translate_workflow(wf, default_params={})
    assert "MockGlueJobOperator" in source
    assert "GlueDockerOperator" not in source


def test_translate_workflow_glue_docker_executor(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "linear_chain")[0]
    source = translate_workflow(
        wf,
        default_params={"OUTPUT_BUCKET": "local-output"},
        executor="glue-docker",
    )
    assert "GlueDockerOperator" in source
    assert "MockGlueJobOperator" not in source
    # Each task carries its script_location (quote style is repr-driven, so check substring).
    assert "s3://x/extract.py" in source
    assert "s3://x/transform.py" in source
    assert "s3://x/load.py" in source
    # script_location is rendered as a kwarg (quote-agnostic check).
    assert source.count("script_location=") == 3
    # default_params is inlined as a dict literal in each task.
    assert "default_params={'OUTPUT_BUCKET': 'local-output'}" in source
    # Compiles
    compile(source, "<dag>", "exec")


def test_translate_workflow_unknown_executor_rejected(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "linear_chain")[0]
    with pytest.raises(ValueError, match="executor"):
        translate_workflow(wf, default_params={}, executor="bogus")
