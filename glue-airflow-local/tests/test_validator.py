"""Tests for validator."""

import pytest

from glue_airflow_local.exceptions import InvalidWorkflowError
from glue_airflow_local.parser import parse_directory
from glue_airflow_local.validator import validate_workflow


def test_linear_chain_validates(fixtures_dir):
    wf = parse_directory(fixtures_dir / "linear_chain")[0]
    validate_workflow(wf)  # no raise


def test_parallel_branches_validates(fixtures_dir):
    wf = parse_directory(fixtures_dir / "parallel_branches")[0]
    validate_workflow(wf)


def test_cycle_detected(fixtures_dir):
    wf = parse_directory(fixtures_dir / "cyclic")[0]
    with pytest.raises(InvalidWorkflowError, match="cycle"):
        validate_workflow(wf)


def test_no_start_trigger_rejected():
    """A workflow with no ON_DEMAND or SCHEDULED entry trigger is unrunnable."""
    from glue_airflow_local.model import (
        Action,
        Condition,
        Job,
        Predicate,
        Trigger,
        TriggerType,
        Workflow,
    )

    wf = Workflow(
        name="orphan",
        triggers=[
            Trigger(
                name="t",
                type=TriggerType.CONDITIONAL,
                actions=[Action(job_name="b")],
                predicate=Predicate(conditions=(Condition(job_name="a"),)),
            )
        ],
        jobs={
            "a": Job(name="a", script_location="s3://x/a.py"),
            "b": Job(name="b", script_location="s3://x/b.py"),
        },
    )
    with pytest.raises(InvalidWorkflowError, match="entry trigger"):
        validate_workflow(wf)
