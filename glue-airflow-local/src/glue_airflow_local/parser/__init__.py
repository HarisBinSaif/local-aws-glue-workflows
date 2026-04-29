"""Public entry points for the Terraform → Glue Workflow IR parser."""

from __future__ import annotations

from pathlib import Path

from glue_airflow_local.exceptions import GlueParseError
from glue_airflow_local.model import Workflow
from glue_airflow_local.parser.glue import extract_workflows
from glue_airflow_local.parser.hcl import read_tf_file

__all__ = ["parse_directory", "parse_files"]


def parse_directory(path: Path | str) -> list[Workflow]:
    """Read every `*.tf` file in the directory (non-recursive) and return Workflows."""
    directory = Path(path)
    if not directory.is_dir():
        raise GlueParseError(f"Not a directory: {directory}")
    tf_files = sorted(directory.glob("*.tf"))
    if not tf_files:
        raise GlueParseError(f"No .tf files in {directory}")
    return parse_files(tf_files)


def parse_files(paths: list[Path]) -> list[Workflow]:
    """Parse a specific list of .tf files."""
    parsed = [read_tf_file(p) for p in paths]
    return extract_workflows(parsed)
