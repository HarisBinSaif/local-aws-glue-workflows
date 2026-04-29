"""Tests for the in-process mock Glue job operator."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from glue_airflow_local.operators.mock import MockGlueJobOperator


@pytest.fixture
def workflow_dir(tmp_path):
    (tmp_path / "default_params.json").write_text(
        json.dumps({"OUTPUT_BUCKET": "s3://x", "MODE": "default"})
    )
    return tmp_path


def _ctx(dag_run_conf: dict | None = None):
    dag_run = MagicMock()
    dag_run.conf = dag_run_conf or {}
    return {"dag_run": dag_run, "ds_nodash": "20260429"}


def test_mock_succeeds_by_default(workflow_dir):
    op = MockGlueJobOperator(
        task_id="t", job_name="extract-job", workflow_dir=str(workflow_dir)
    )
    result = op.execute(_ctx())
    assert result["job_name"] == "extract-job"
    assert result["status"] == "SUCCEEDED"
    assert result["params"]["OUTPUT_BUCKET"] == "s3://x"


def test_mock_dagrun_conf_overrides_defaults(workflow_dir):
    op = MockGlueJobOperator(
        task_id="t", job_name="j", workflow_dir=str(workflow_dir)
    )
    result = op.execute(_ctx({"MODE": "override"}))
    assert result["params"]["MODE"] == "override"
    assert result["params"]["OUTPUT_BUCKET"] == "s3://x"


def test_mock_can_be_configured_to_fail(workflow_dir):
    op = MockGlueJobOperator(
        task_id="t", job_name="j", workflow_dir=str(workflow_dir), behavior="fail"
    )
    with pytest.raises(RuntimeError, match="configured to fail"):
        op.execute(_ctx())


def test_mock_can_sleep_then_succeed(workflow_dir):
    op = MockGlueJobOperator(
        task_id="t",
        job_name="j",
        workflow_dir=str(workflow_dir),
        behavior="sleep",
        sleep_seconds=0.01,
    )
    result = op.execute(_ctx())
    assert result["status"] == "SUCCEEDED"


def test_mock_unknown_behavior_rejected(workflow_dir):
    with pytest.raises(ValueError, match="behavior"):
        MockGlueJobOperator(
            task_id="t",
            job_name="j",
            workflow_dir=str(workflow_dir),
            behavior="explode",
        )
