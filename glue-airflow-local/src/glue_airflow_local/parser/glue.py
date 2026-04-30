"""Extract AWS Glue Workflow IR from parsed Terraform documents."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

from glue_airflow_local.exceptions import GlueParseError, UnsupportedTriggerError
from glue_airflow_local.model import (
    Action,
    Condition,
    Job,
    JobState,
    LogicalOperator,
    Predicate,
    Trigger,
    TriggerType,
    Workflow,
)
from glue_airflow_local.parser.references import ResourceTable, resolve_string

_GLUE_RESOURCE_TYPES = {"aws_glue_workflow", "aws_glue_trigger", "aws_glue_job"}


def _iter_resources(
    parsed_files: Iterable[dict[str, Any]],
) -> Iterator[tuple[str, str, dict[str, Any]]]:
    """Yield (resource_type, logical_name, attrs) across all parsed files.

    Non-Glue resources are skipped via :data:`_GLUE_RESOURCE_TYPES` so callers do
    not have to filter the broader Terraform document themselves.
    """
    for parsed in parsed_files:
        for entry in parsed.get("resource", []):
            for resource_type, by_name in entry.items():
                if resource_type not in _GLUE_RESOURCE_TYPES:
                    continue
                for logical_name, attrs in by_name.items():
                    yield resource_type, logical_name, attrs


def _build_resource_table(parsed_files: Iterable[dict[str, Any]]) -> ResourceTable:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for resource_type, logical_name, attrs in _iter_resources(parsed_files):
        by_key[(resource_type, logical_name)] = attrs
    return ResourceTable(by_key)


def _block_first(block: Any) -> dict[str, Any]:
    """python-hcl2 returns nested blocks as either a list[dict] or a dict -- normalise."""
    if isinstance(block, list):
        if not block:
            raise GlueParseError("Empty nested block")
        first = block[0]
        if not isinstance(first, dict):
            raise GlueParseError(f"Unexpected block shape: {type(first).__name__}")
        return first
    if isinstance(block, dict):
        return block
    raise GlueParseError(f"Unexpected block shape: {type(block).__name__}")


def _block_all(block: Any) -> list[dict[str, Any]]:
    """Like _block_first, but returns all entries (used for `actions` / `conditions`)."""
    if isinstance(block, list):
        result: list[dict[str, Any]] = []
        for item in block:
            if not isinstance(item, dict):
                raise GlueParseError(f"Unexpected block shape: {type(item).__name__}")
            result.append(item)
        return result
    if isinstance(block, dict):
        return [block]
    raise GlueParseError(f"Unexpected block shape: {type(block).__name__}")


def _extract_actions(attrs: dict[str, Any], table: ResourceTable) -> list[Action]:
    if "actions" not in attrs:
        raise GlueParseError("Trigger missing 'actions' block")
    actions: list[Action] = []
    for action_block in _block_all(attrs["actions"]):
        if "job_name" not in action_block:
            raise UnsupportedTriggerError(
                "Only job actions are supported; crawler actions are deferred to a later release"
            )
        actions.append(Action(job_name=resolve_string(str(action_block["job_name"]), table)))
    return actions


def _extract_predicate(attrs: dict[str, Any], table: ResourceTable) -> Predicate:
    if "predicate" not in attrs:
        raise GlueParseError("CONDITIONAL trigger missing 'predicate' block")
    pred_block = _block_first(attrs["predicate"])
    if "conditions" not in pred_block:
        raise GlueParseError("Predicate missing 'conditions' block")

    raw_logical = str(pred_block.get("logical", "AND")).upper()
    try:
        logical = LogicalOperator(raw_logical)
    except ValueError as exc:
        raise GlueParseError(f"Unknown predicate logical operator: {raw_logical}") from exc

    conditions: list[Condition] = []
    for cond_block in _block_all(pred_block["conditions"]):
        job_name = resolve_string(str(cond_block.get("job_name", "")), table)
        if not job_name:
            raise GlueParseError("Predicate condition missing job_name")
        raw_state = str(cond_block.get("state", "SUCCEEDED")).upper()
        try:
            state = JobState(raw_state)
        except ValueError as exc:
            raise GlueParseError(f"Unknown job state in predicate: {raw_state}") from exc
        conditions.append(Condition(job_name=job_name, state=state))

    return Predicate(conditions=tuple(conditions), logical=logical)


def _extract_jobs(parsed_files: list[dict[str, Any]], table: ResourceTable) -> dict[str, Job]:
    """Build a name -> Job map from all aws_glue_job resources across the parsed files."""
    jobs: dict[str, Job] = {}
    for resource_type, logical_name, attrs in _iter_resources(parsed_files):
        if resource_type != "aws_glue_job":
            continue
        name = resolve_string(str(attrs.get("name", logical_name)), table)
        command_block = attrs.get("command")
        if command_block is None:
            raise GlueParseError(f"aws_glue_job {logical_name!r} missing 'command' block")
        command = _block_first(command_block)
        script_raw = command.get("script_location")
        if script_raw is None:
            raise GlueParseError(
                f"aws_glue_job {logical_name!r}: command.script_location is required"
            )
        script_location = resolve_string(str(script_raw), table)
        jobs[name] = Job(name=name, script_location=script_location)
    return jobs


def _extract_trigger(
    logical_name: str, attrs: dict[str, Any], table: ResourceTable
) -> tuple[str, Trigger]:
    """Returns (workflow_name, trigger). workflow_name is the resolved name."""
    raw_type = str(attrs.get("type", "")).upper()
    try:
        trigger_type = TriggerType(raw_type)
    except ValueError as exc:
        raise UnsupportedTriggerError(
            f"Trigger {logical_name!r} has unsupported type {raw_type!r}"
        ) from exc

    workflow_name_raw = attrs.get("workflow_name")
    if workflow_name_raw is None:
        raise GlueParseError(f"Trigger {logical_name!r} missing workflow_name")
    workflow_name = resolve_string(str(workflow_name_raw), table)

    actions = _extract_actions(attrs, table)
    predicate: Predicate | None = None
    if trigger_type is TriggerType.CONDITIONAL:
        predicate = _extract_predicate(attrs, table)

    schedule: str | None = None
    if trigger_type is TriggerType.SCHEDULED:
        sched_raw = attrs.get("schedule")
        if sched_raw is None:
            raise GlueParseError(f"Trigger {logical_name!r}: SCHEDULED needs 'schedule'")
        schedule = resolve_string(str(sched_raw), table)

    name = resolve_string(str(attrs.get("name", logical_name)), table)
    return workflow_name, Trigger(
        name=name,
        type=trigger_type,
        actions=actions,
        predicate=predicate,
        schedule=schedule,
    )


def extract_workflows(parsed_files: Iterable[dict[str, Any]]) -> list[Workflow]:
    """Build Workflow IR from a sequence of parsed .tf documents."""
    parsed_list = list(parsed_files)
    table = _build_resource_table(parsed_list)
    all_jobs = _extract_jobs(parsed_list, table)

    workflows_by_name: dict[str, Workflow] = {}
    for resource_type, logical_name, attrs in _iter_resources(parsed_list):
        if resource_type != "aws_glue_workflow":
            continue
        wf_name = resolve_string(str(attrs.get("name", logical_name)), table)
        description = attrs.get("description")
        if description is not None:
            description = resolve_string(str(description), table)
        workflows_by_name[wf_name] = Workflow(
            name=wf_name, triggers=[], description=description
        )

    if not workflows_by_name:
        raise GlueParseError("No aws_glue_workflow resources found")

    for resource_type, logical_name, attrs in _iter_resources(parsed_list):
        if resource_type != "aws_glue_trigger":
            continue
        wf_name, trigger = _extract_trigger(logical_name, attrs, table)
        if wf_name not in workflows_by_name:
            raise GlueParseError(
                f"Trigger {trigger.name!r} references unknown workflow {wf_name!r}"
            )
        workflow = workflows_by_name[wf_name]
        workflow.triggers.append(trigger)
        for action in trigger.actions:
            if action.job_name not in all_jobs:
                raise GlueParseError(
                    f"Trigger {trigger.name!r} references unknown job {action.job_name!r} "
                    f"(no aws_glue_job resource defines it)"
                )
            workflow.jobs[action.job_name] = all_jobs[action.job_name]

    return list(workflows_by_name.values())
