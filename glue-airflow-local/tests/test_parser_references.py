"""Test Terraform reference resolution."""

import pytest

from glue_airflow_local.exceptions import UnresolvedReferenceError
from glue_airflow_local.parser.references import (
    ResourceTable,
    resolve_string,
)


@pytest.fixture
def table() -> ResourceTable:
    return ResourceTable(
        {
            ("aws_glue_job", "extract"): {"name": "extract-job"},
            ("aws_glue_workflow", "etl"): {"name": "etl-workflow"},
        }
    )


def test_resolve_plain_string_passthrough(table):
    assert resolve_string("hello", table) == "hello"


def test_resolve_simple_reference(table):
    assert resolve_string("${aws_glue_job.extract.name}", table) == "extract-job"


def test_resolve_workflow_name_reference(table):
    assert (
        resolve_string("${aws_glue_workflow.etl.name}", table) == "etl-workflow"
    )


def test_resolve_unknown_resource_raises(table):
    with pytest.raises(UnresolvedReferenceError, match=r"aws_glue_job.missing"):
        resolve_string("${aws_glue_job.missing.name}", table)


def test_resolve_unknown_attribute_raises(table):
    with pytest.raises(UnresolvedReferenceError, match="role_arn"):
        resolve_string("${aws_glue_job.extract.role_arn}", table)


def test_resolve_non_name_attribute_returns_string():
    """Some attrs other than 'name' are reasonable to look up too."""
    table = ResourceTable({("aws_glue_job", "x"): {"name": "x-job", "max_retries": 3}})
    assert resolve_string("${aws_glue_job.x.max_retries}", table) == "3"
