"""Resolve Terraform interpolation references in attribute values.

Example: "${aws_glue_job.extract.name}" -> "extract-job"
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from glue_airflow_local.exceptions import UnresolvedReferenceError

_REF_RE = re.compile(
    r"^\$\{(?P<type>[a-zA-Z_][\w]*)\.(?P<name>[a-zA-Z_][\w-]*)\.(?P<attr>[\w]+)\}$"
)


@dataclass
class ResourceTable:
    """Maps (resource_type, logical_name) -> attribute dict."""

    by_key: dict[tuple[str, str], dict[str, Any]]

    def lookup(self, resource_type: str, logical_name: str, attr: str) -> Any:
        key = (resource_type, logical_name)
        if key not in self.by_key:
            raise UnresolvedReferenceError(
                f"Unknown reference: {resource_type}.{logical_name}"
            )
        attrs = self.by_key[key]
        if attr not in attrs:
            raise UnresolvedReferenceError(
                f"Resource {resource_type}.{logical_name} has no attribute {attr!r}"
            )
        return attrs[attr]


def resolve_string(value: str, table: ResourceTable) -> str:
    """Resolve a single attribute string. Plain strings pass through."""
    match = _REF_RE.match(value)
    if not match:
        return value
    looked_up = table.lookup(match["type"], match["name"], match["attr"])
    return str(looked_up)
