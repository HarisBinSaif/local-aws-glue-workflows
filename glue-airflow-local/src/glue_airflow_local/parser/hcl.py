"""Thin wrapper around python-hcl2 that returns a typed dict for one .tf file."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import hcl2

from glue_airflow_local.exceptions import GlueParseError


def _strip_outer_quotes(s: str) -> str:
    """Remove a single pair of surrounding double-quotes if present."""
    if len(s) >= 2 and s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


def _normalize(value: Any) -> Any:
    """Strip python-hcl2 8.x artifacts: outer quotes on labels/strings and __is_block__ sentinels.

    Leaves ``${...}`` interpolation strings, list-wrapping of nested blocks, and non-string
    scalar values untouched.
    """
    if isinstance(value, dict):
        return {
            _strip_outer_quotes(k): _normalize(v)
            for k, v in value.items()
            if k != "__is_block__"
        }
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, str):
        return _strip_outer_quotes(value)
    return value


def read_tf_file(path: Path) -> dict[str, Any]:
    """Parse a single Terraform file and return a normalized HCL2 dict.

    The returned shape follows python-hcl2 conventions, but is normalized:
    outer double-quotes are stripped from labels and string values, and the
    ``__is_block__`` sentinel keys that python-hcl2 8.x injects are removed.
    ``${...}`` interpolation strings, list-wrapping of nested blocks, and
    non-string scalar values are left untouched.

    Example normalized shape::

        {
            "resource": [
                {"<type>": {"<logical_name>": {<attrs>}}},
                ...
            ],
            "variable": [...],
            ...
        }
    """
    if not path.is_file():
        raise GlueParseError(f"Terraform file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = hcl2.load(fh)
    except Exception as exc:  # python-hcl2 raises a variety of types
        raise GlueParseError(f"Failed to parse {path}: {exc}") from exc
    return cast(dict[str, Any], _normalize(raw))
