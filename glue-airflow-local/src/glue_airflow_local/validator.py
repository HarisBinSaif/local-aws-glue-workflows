"""Structural checks on a Workflow IR before translation."""

from __future__ import annotations

from collections import defaultdict

from glue_airflow_local.exceptions import InvalidWorkflowError
from glue_airflow_local.model import TriggerType, Workflow

# DFS colours for cycle detection.
_WHITE = 0  # unvisited
_GREY = 1  # on the current DFS path
_BLACK = 2  # fully explored


def validate_workflow(workflow: Workflow) -> None:
    """Raise InvalidWorkflowError if the workflow can't be translated to a runnable DAG."""
    _check_no_cycles(workflow)
    _require_entry_trigger(workflow)


def _require_entry_trigger(workflow: Workflow) -> None:
    has_entry = any(
        t.type in (TriggerType.ON_DEMAND, TriggerType.SCHEDULED) for t in workflow.triggers
    )
    if not has_entry:
        raise InvalidWorkflowError(
            f"Workflow {workflow.name!r} has no entry trigger (ON_DEMAND or SCHEDULED)"
        )


def _check_no_cycles(workflow: Workflow) -> None:
    """Build the job-level dependency graph from CONDITIONAL triggers and DFS for cycles."""
    deps: dict[str, set[str]] = defaultdict(set)
    for trig in workflow.triggers:
        if trig.type is TriggerType.CONDITIONAL and trig.predicate is not None:
            upstream_jobs = {c.job_name for c in trig.predicate.conditions}
            for action in trig.actions:
                deps[action.job_name].update(upstream_jobs)

    colour: dict[str, int] = defaultdict(lambda: _WHITE)

    def visit(node: str, path: list[str]) -> None:
        if colour[node] == _GREY:
            cycle = " -> ".join([*path, node])
            raise InvalidWorkflowError(f"Workflow {workflow.name!r} contains a cycle: {cycle}")
        if colour[node] == _BLACK:
            return
        colour[node] = _GREY
        for upstream in deps.get(node, ()):
            visit(upstream, [*path, node])
        colour[node] = _BLACK

    for job in workflow.jobs:
        if colour[job] == _WHITE:
            visit(job, [])
