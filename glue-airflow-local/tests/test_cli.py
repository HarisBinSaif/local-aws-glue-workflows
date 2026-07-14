"""Tests for the glue-airflow-local CLI."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from glue_airflow_local.cli import main


def test_translate_writes_dag_file(fixtures_dir, tmp_path, capsys):
    out = tmp_path / "dag.py"
    rc = main(["translate", str(fixtures_dir / "linear_chain"), "--output", str(out)])
    assert rc == 0
    assert out.is_file()
    text = out.read_text()
    assert 'dag_id="linear-etl"' in text


def test_translate_glue_docker_executor_writes_dag_with_glue_docker(fixtures_dir, tmp_path):
    out = tmp_path / "dag.py"
    rc = main([
        "translate",
        str(fixtures_dir / "linear_chain"),
        "--output", str(out),
        "--executor", "glue-docker",
    ])
    assert rc == 0
    text = out.read_text()
    assert "GlueDockerOperator" in text
    assert "MockGlueJobOperator" not in text


def test_translate_unknown_executor_rejected_by_argparse(fixtures_dir, tmp_path):
    out = tmp_path / "dag.py"
    with pytest.raises(SystemExit) as exc_info:
        main([
            "translate",
            str(fixtures_dir / "linear_chain"),
            "--output", str(out),
            "--executor", "bogus",
        ])
    # argparse exits with code 2 on invalid choices
    assert exc_info.value.code == 2


def test_cli_passes_terraform_params_with_json_override(fixtures_dir, tmp_path):
    """Terraform-declared params reach the generated DAG; default_params.json overrides them."""
    out = tmp_path / "dag.py"
    # The fixture sets ENV at workflow, job, and trigger levels.
    # JSON overrides ENV with 'json-env' and adds JSON_LEVEL.
    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()
    (workflow_dir / "default_params.json").write_text(
        json.dumps({"ENV": "json-env", "JSON_LEVEL": "yes"})
    )
    rc = main(
        [
            "translate",
            str(fixtures_dir / "with_default_args"),
            "--output", str(out),
            "--workflow-dir", str(workflow_dir),
        ]
    )
    assert rc == 0
    text = out.read_text()
    # JSON wins for ENV.
    assert "'ENV': 'json-env'" in text
    # Workflow-declared key is present.
    assert "'OUTPUT_BUCKET': 'wf-bucket'" in text
    # Job-declared key is present (extract job only).
    assert "'JOB_LEVEL': 'yes'" in text
    # Trigger-declared key is present (extract job only, fired from 'start').
    assert "'TRIGGER_LEVEL': 'yes'" in text
    # JSON addition is present.
    assert "'JSON_LEVEL': 'yes'" in text


def test_cli_invocable_via_module(fixtures_dir, tmp_path):
    """End-to-end: spawn `python -m glue_airflow_local` like a real user would."""
    out = tmp_path / "dag.py"
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "glue_airflow_local",
            "translate",
            str(fixtures_dir / "linear_chain"),
            "--output",
            str(out),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert out.is_file()
