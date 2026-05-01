"""Tests for the GlueDockerOperator (real-execution mode).

All tests mock the Docker SDK — no real container required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from glue_airflow_local.operators.glue_docker import (
    GlueDockerOperator,
    _build_spark_submit_argv,
    _resolve_script_path,
)


def _ctx(dag_run_conf: dict | None = None) -> dict:
    dag_run = MagicMock()
    dag_run.conf = dag_run_conf or {}
    dag_run.run_id = "manual__2026-04-29T12:00:00"
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


def test_resolve_script_path_rejects_empty_key():
    with pytest.raises(ValueError, match="no key after bucket"):
        _resolve_script_path("s3://only-bucket/", "/scripts")


def test_resolve_script_path_rejects_bucket_only():
    with pytest.raises(ValueError, match="no key after bucket"):
        _resolve_script_path("s3://only-bucket", "/scripts")


def test_build_spark_submit_argv_includes_job_name_and_params():
    argv = _build_spark_submit_argv(
        script_path="/scripts/extract.py",
        job_name="extract-job",
        workflow_run_id="manual__2026-04-29T12:00:00",
        params={"OUTPUT_BUCKET": "local-output", "ENV": "local"},
    )
    assert argv[0] == "spark-submit"
    assert argv[1] == "/scripts/extract.py"
    assert "--JOB_NAME=extract-job" in argv
    assert "--WORKFLOW_RUN_ID=manual__2026-04-29T12:00:00" in argv
    assert "--OUTPUT_BUCKET=local-output" in argv
    assert "--ENV=local" in argv


def test_build_spark_submit_argv_is_deterministic_in_param_order():
    argv = _build_spark_submit_argv(
        script_path="/scripts/x.py",
        job_name="x",
        workflow_run_id="r1",
        params={"BBB": "2", "AAA": "1"},
    )
    a_idx = argv.index("--AAA=1")
    b_idx = argv.index("--BBB=2")
    assert a_idx < b_idx


def test_executes_via_docker_exec_and_returns_status(monkeypatch):
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
        default_params={"OUTPUT_BUCKET": "local-output"},
        container_name="glue-runner",
    )
    result = op.execute(_ctx())

    fake_client.containers.get.assert_called_once_with("glue-runner")
    args, kwargs = fake_container.exec_run.call_args
    cmd = args[0] if args else kwargs.get("cmd")
    assert cmd[0] == "spark-submit"
    assert "/scripts/scripts/extract.py" in cmd
    assert "--JOB_NAME=extract-job" in cmd
    assert "--OUTPUT_BUCKET=local-output" in cmd
    # workflow_run_id sourced from dag_run.run_id (always unique).
    assert "--WORKFLOW_RUN_ID=manual__2026-04-29T12:00:00" in cmd

    assert result["status"] == "SUCCEEDED"
    assert result["job_name"] == "extract-job"
    assert result["exit_code"] == 0


def test_executes_raises_on_nonzero_exit(monkeypatch):
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
    )
    with pytest.raises(RuntimeError, match="exit code 1"):
        op.execute(_ctx())


def test_dagrun_conf_overrides_default_params(monkeypatch):
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
        default_params={"OUTPUT_BUCKET": "local-output", "ENV": "default"},
    )
    op.execute(_ctx({"ENV": "override"}))

    cmd = seen_cmd[0]
    assert "--ENV=override" in cmd
    assert "--ENV=default" not in cmd
    assert "--OUTPUT_BUCKET=local-output" in cmd


def test_executes_wraps_container_not_found(monkeypatch):
    """If the glue-runner container isn't running, surface a friendly RuntimeError."""
    import docker.errors

    fake_client = MagicMock()
    fake_client.containers.get.side_effect = docker.errors.NotFound("no such container")

    monkeypatch.setattr(
        "glue_airflow_local.operators.glue_docker.docker.from_env",
        lambda: fake_client,
    )

    op = GlueDockerOperator(
        task_id="t",
        job_name="j",
        script_location="s3://b/j.py",
    )
    with pytest.raises(RuntimeError, match=r"docker compose .* up -d"):
        op.execute(_ctx())
