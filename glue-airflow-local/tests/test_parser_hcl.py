"""Test the lowest-level HCL2 reader."""

from __future__ import annotations

import pytest

from glue_airflow_local.exceptions import GlueParseError
from glue_airflow_local.parser.hcl import _normalize, _strip_outer_quotes, read_tf_file


def test_read_returns_resource_section(fixtures_dir):
    parsed = read_tf_file(fixtures_dir / "single_ondemand" / "main.tf")
    assert "resource" in parsed
    assert isinstance(parsed["resource"], list)
    types_seen = set()
    for entry in parsed["resource"]:
        types_seen.update(entry.keys())
    assert {"aws_glue_workflow", "aws_glue_job", "aws_glue_trigger"} <= types_seen


def test_read_missing_file_raises(tmp_path):
    with pytest.raises(GlueParseError, match="not found"):
        read_tf_file(tmp_path / "nope.tf")


def test_strip_outer_quotes_removes_one_pair():
    assert _strip_outer_quotes('"hello"') == "hello"


def test_strip_outer_quotes_leaves_unquoted():
    assert _strip_outer_quotes("hello") == "hello"
    assert _strip_outer_quotes("${ref.name.attr}") == "${ref.name.attr}"


def test_strip_outer_quotes_handles_empty_quoted():
    assert _strip_outer_quotes('""') == ""


def test_normalize_strips_quoted_keys_and_values():
    raw = {'"type"': {'"name"': {"key": '"value"'}}}
    assert _normalize(raw) == {"type": {"name": {"key": "value"}}}


def test_normalize_drops_is_block_sentinels():
    raw = {"name": '"x"', "__is_block__": True}
    assert _normalize(raw) == {"name": "x"}


def test_normalize_recurses_into_lists():
    raw = [{'"key"': '"value"', "__is_block__": True}, {"plain": 42}]
    assert _normalize(raw) == [{"key": "value"}, {"plain": 42}]


def test_normalize_preserves_interpolation_strings():
    raw = {"workflow_name": "${aws_glue_workflow.etl.name}"}
    assert _normalize(raw) == {"workflow_name": "${aws_glue_workflow.etl.name}"}


def test_normalize_idempotent():
    raw = {'"a"': [{'"b"': '"c"', "__is_block__": True}]}
    once = _normalize(raw)
    twice = _normalize(once)
    assert once == twice
