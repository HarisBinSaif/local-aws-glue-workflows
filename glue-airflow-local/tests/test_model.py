"""Tests for the IR data model."""

import pytest

from glue_airflow_local.exceptions import InvalidWorkflowError
from glue_airflow_local.model import (
    Action,
    Condition,
    Job,
    Predicate,
    Trigger,
    TriggerType,
    Workflow,
)


def test_trigger_ondemand_must_not_have_predicate():
    with pytest.raises(InvalidWorkflowError, match=r"ON_DEMAND.*predicate"):
        Trigger(
            name="t",
            type=TriggerType.ON_DEMAND,
            actions=[Action(job_name="j")],
            predicate=Predicate(conditions=(Condition(job_name="x"),)),
        )


def test_trigger_conditional_must_have_predicate():
    with pytest.raises(InvalidWorkflowError, match=r"CONDITIONAL.*predicate"):
        Trigger(name="t", type=TriggerType.CONDITIONAL, actions=[Action(job_name="j")])


def test_trigger_scheduled_must_have_schedule():
    with pytest.raises(InvalidWorkflowError, match=r"SCHEDULED.*schedule"):
        Trigger(name="t", type=TriggerType.SCHEDULED, actions=[Action(job_name="j")])


def test_trigger_ondemand_must_not_have_schedule():
    with pytest.raises(InvalidWorkflowError, match=r"ON_DEMAND.*schedule"):
        Trigger(
            name="t",
            type=TriggerType.ON_DEMAND,
            actions=[Action(job_name="j")],
            schedule="cron(0 2 * * ? *)",
        )


def test_workflow_construct_minimal():
    wf = Workflow(
        name="my-wf",
        triggers=[
            Trigger(
                name="start",
                type=TriggerType.ON_DEMAND,
                actions=[Action(job_name="extract")],
            )
        ],
        jobs={"extract": Job(name="extract", script_location="s3://bucket/extract.py")},
    )
    assert wf.name == "my-wf"
    assert wf.jobs["extract"].script_location == "s3://bucket/extract.py"
