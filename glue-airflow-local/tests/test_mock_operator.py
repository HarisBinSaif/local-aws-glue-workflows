"""Tests for the in-process mock Glue job operator."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from glue_airflow_local.operators.mock import MockGlueJobOperator


def _ctx(dag_run_conf: dict | None = None):
    dag_run = MagicMock()
    dag_run.conf = dag_run_conf or {}
    return {"dag_run": dag_run, "ds_nodash": "20260429"}


def test_mock_succeeds_by_default():
    op = MockGlueJobOperator(
        task_id="t",
        job_name="extract-job",
        default_params={"OUTPUT_BUCKET": "s3://x", "MODE": "default"},
    )
    result = op.execute(_ctx())
    assert result["job_name"] == "extract-job"
    assert result["status"] == "SUCCEEDED"
    assert result["params"]["OUTPUT_BUCKET"] == "s3://x"


def test_mock_dagrun_conf_overrides_defaults():
    op = MockGlueJobOperator(
        task_id="t",
        job_name="j",
        default_params={"OUTPUT_BUCKET": "s3://x", "MODE": "default"},
    )
    result = op.execute(_ctx({"MODE": "override"}))
    assert result["params"]["MODE"] == "override"
    assert result["params"]["OUTPUT_BUCKET"] == "s3://x"


def test_mock_can_be_configured_to_fail():
    op = MockGlueJobOperator(
        task_id="t", job_name="j", default_params={}, behavior="fail",
    )
    with pytest.raises(RuntimeError, match="configured to fail"):
        op.execute(_ctx())


def test_mock_can_sleep_then_succeed():
    op = MockGlueJobOperator(
        task_id="t",
        job_name="j",
        default_params={},
        behavior="sleep",
        sleep_seconds=0.01,
    )
    result = op.execute(_ctx())
    assert result["status"] == "SUCCEEDED"


def test_mock_unknown_behavior_rejected():
    with pytest.raises(ValueError, match="behavior"):
        MockGlueJobOperator(
            task_id="t",
            job_name="j",
            default_params={},
            behavior="explode",
        )


def test_mock_default_params_optional():
    """Constructing with no default_params is fine; merged params are just the dag_run conf."""
    op = MockGlueJobOperator(task_id="t", job_name="j")
    result = op.execute(_ctx({"X": "1"}))
    assert result["params"] == {"X": "1"}
