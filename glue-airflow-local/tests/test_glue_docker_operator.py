"""Tests for the GlueDockerOperator (Plan B real-execution mode).

All tests mock the Docker SDK — no real container required.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from glue_airflow_local.operators.glue_docker import (
    GlueDockerOperator,
    _build_spark_submit_argv,
    _resolve_script_path,
)


@pytest.fixture
def workflow_dir(tmp_path: Path) -> Path:
    (tmp_path / "default_params.json").write_text(
        json.dumps({"OUTPUT_BUCKET": "local-output", "ENV": "local"})
    )
    return tmp_path


def _ctx(dag_run_conf: dict | None = None) -> dict:
    dag_run = MagicMock()
    dag_run.conf = dag_run_conf or {}
    return {"dag_run": dag_run, "ds_nodash": "20260429"}


def test_resolve_script_path_strips_s3_prefix():
    assert (
        _resolve_script_path("s3://example-bucket/scripts/extract.py", "/scripts")
        == "/scripts/scripts/extract.py"
    )


def test_resolve_script_path_strips_only_known_prefix():
    assert (
        _resolve_script_path("s3://b/extract.py", "/scripts")
        == "/scripts/extract.py"
    )


def test_resolve_script_path_rejects_non_s3():
    with pytest.raises(ValueError, match="must start with s3://"):
        _resolve_script_path("/local/extract.py", "/scripts")


def test_build_spark_submit_argv_includes_job_name_and_params():
    argv = _build_spark_submit_argv(
        script_path="/scripts/extract.py",
        job_name="extract-job",
        workflow_run_id="20260429",
        params={"OUTPUT_BUCKET": "local-output", "ENV": "local"},
    )
    # Layout: spark-submit + script + --JOB_NAME + --WORKFLOW_RUN_ID + each --K=V param
    assert argv[0] == "spark-submit"
    assert argv[1] == "/scripts/extract.py"
    assert "--JOB_NAME=extract-job" in argv
    assert "--WORKFLOW_RUN_ID=20260429" in argv
    assert "--OUTPUT_BUCKET=local-output" in argv
    assert "--ENV=local" in argv


def test_build_spark_submit_argv_is_deterministic_in_param_order():
    """Sorted param order so generated commands are reproducible."""
    argv = _build_spark_submit_argv(
        script_path="/scripts/x.py",
        job_name="x",
        workflow_run_id="20260429",
        params={"BBB": "2", "AAA": "1"},
    )
    # AAA appears before BBB
    a_idx = argv.index("--AAA=1")
    b_idx = argv.index("--BBB=2")
    assert a_idx < b_idx


def test_executes_via_docker_exec_and_returns_status(workflow_dir, monkeypatch):
    fake_exec_result = MagicMock()
    fake_exec_result.exit_code = 0
    fake_exec_result.output = b"job output line 1\njob output line 2\n"

    fake_container = MagicMock()
    fake_container.exec_run.return_value = fake_exec_result

    fake_client = MagicMock()
    fake_client.containers.get.return_value = fake_container

    monkeypatch.setattr(
        "glue_airflow_local.operators.glue_docker.docker.from_env",
        lambda: fake_client,
    )

    op = GlueDockerOperator(
        task_id="t",
        job_name="extract-job",
        script_location="s3://example-bucket/scripts/extract.py",
        workflow_dir=str(workflow_dir),
        container_name="glue-runner",
    )
    result = op.execute(_ctx())

    fake_client.containers.get.assert_called_once_with("glue-runner")
    args, kwargs = fake_container.exec_run.call_args
    cmd = args[0] if args else kwargs.get("cmd")
    assert cmd[0] == "spark-submit"
    assert "/scripts/scripts/extract.py" in cmd
    assert "--JOB_NAME=extract-job" in cmd

    assert result["status"] == "SUCCEEDED"
    assert result["job_name"] == "extract-job"
    assert result["exit_code"] == 0


def test_executes_raises_on_nonzero_exit(workflow_dir, monkeypatch):
    fake_exec_result = MagicMock()
    fake_exec_result.exit_code = 1
    fake_exec_result.output = b"Traceback (most recent call last)\n... ValueError\n"

    fake_container = MagicMock()
    fake_container.exec_run.return_value = fake_exec_result

    fake_client = MagicMock()
    fake_client.containers.get.return_value = fake_container

    monkeypatch.setattr(
        "glue_airflow_local.operators.glue_docker.docker.from_env",
        lambda: fake_client,
    )

    op = GlueDockerOperator(
        task_id="t",
        job_name="x",
        script_location="s3://b/x.py",
        workflow_dir=str(workflow_dir),
    )
    with pytest.raises(RuntimeError, match="exit code 1"):
        op.execute(_ctx())


def test_dagrun_conf_overrides_default_params(workflow_dir, monkeypatch):
    seen_cmd: list[list[str]] = []

    def capture_exec(cmd, **_):
        seen_cmd.append(cmd)
        result = MagicMock()
        result.exit_code = 0
        result.output = b""
        return result

    fake_container = MagicMock()
    fake_container.exec_run.side_effect = capture_exec
    fake_client = MagicMock()
    fake_client.containers.get.return_value = fake_container

    monkeypatch.setattr(
        "glue_airflow_local.operators.glue_docker.docker.from_env",
        lambda: fake_client,
    )

    op = GlueDockerOperator(
        task_id="t",
        job_name="j",
        script_location="s3://b/j.py",
        workflow_dir=str(workflow_dir),
    )
    op.execute(_ctx({"ENV": "override-env"}))

    cmd = seen_cmd[0]
    assert "--ENV=override-env" in cmd
    assert "--ENV=local" not in cmd
    assert "--OUTPUT_BUCKET=local-output" in cmd  # default still flows through
