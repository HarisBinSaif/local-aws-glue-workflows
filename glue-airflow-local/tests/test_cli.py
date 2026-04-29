"""Tests for the glue-airflow-local CLI."""

from __future__ import annotations

import subprocess
import sys

from glue_airflow_local.cli import main


def test_translate_writes_dag_file(fixtures_dir, tmp_path, capsys):
    out = tmp_path / "dag.py"
    rc = main(["translate", str(fixtures_dir / "linear_chain"), "--output", str(out)])
    assert rc == 0
    assert out.is_file()
    text = out.read_text()
    assert 'dag_id="linear-etl"' in text


def test_translate_glue_docker_executor_errors(fixtures_dir, tmp_path):
    out = tmp_path / "dag.py"
    rc = main([
        "translate",
        str(fixtures_dir / "linear_chain"),
        "--output", str(out),
        "--executor", "glue-docker",
    ])
    assert rc != 0


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
