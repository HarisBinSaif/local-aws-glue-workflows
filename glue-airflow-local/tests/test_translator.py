"""Tests for IR -> Airflow DAG translation."""

from __future__ import annotations

import ast
import keyword as _kw

import pytest

from glue_airflow_local.exceptions import UnsupportedTriggerError
from glue_airflow_local.model import (
    Action,
    Condition,
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
    source = translate_workflow(wf, workflow_dir=str(tmp_path))
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
    source = translate_workflow(wf, workflow_dir=str(tmp_path))
    # Cron expression preserved verbatim
    assert "cron(0 2 * * ? *)" in source


def test_parallel_branches_dag(fixtures_dir, tmp_path):
    wf = parse_directory(fixtures_dir / "parallel_branches")[0]
    source = translate_workflow(wf, workflow_dir=str(tmp_path))
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
        jobs={"foo-bar", "foo_bar"},
    )
    source = translate_workflow(wf, workflow_dir=str(tmp_path))
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
        jobs={"a", "b", "c"},
    )
    with pytest.raises(UnsupportedTriggerError, match="OR predicate"):
        translate_workflow(wf, workflow_dir=str(tmp_path))


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
        jobs={"a", "b"},
    )
    with pytest.raises(UnsupportedTriggerError, match=r"state 'FAILED'"):
        translate_workflow(wf, workflow_dir=str(tmp_path))
