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


def test_workflow_default_run_properties_default_empty():
    wf = Workflow(
        name="w",
        triggers=[
            Trigger(name="t", type=TriggerType.ON_DEMAND, actions=[Action(job_name="j")])
        ],
        jobs={"j": Job(name="j", script_location="s3://x/j.py")},
    )
    assert wf.default_run_properties == {}


def test_workflow_default_run_properties_explicit():
    wf = Workflow(
        name="w",
        triggers=[
            Trigger(name="t", type=TriggerType.ON_DEMAND, actions=[Action(job_name="j")])
        ],
        jobs={"j": Job(name="j", script_location="s3://x/j.py")},
        default_run_properties={"OUTPUT_BUCKET": "prod"},
    )
    assert wf.default_run_properties == {"OUTPUT_BUCKET": "prod"}


def test_job_default_arguments_default_empty():
    j = Job(name="j", script_location="s3://x/j.py")
    assert j.default_arguments == {}


def test_job_default_arguments_explicit():
    j = Job(name="j", script_location="s3://x/j.py", default_arguments={"ENV": "prod"})
    assert j.default_arguments == {"ENV": "prod"}


def test_action_arguments_default_empty():
    a = Action(job_name="j")
    assert a.arguments == {}


def test_action_arguments_explicit():
    a = Action(job_name="j", arguments={"MODE": "incremental"})
    assert a.arguments == {"MODE": "incremental"}
