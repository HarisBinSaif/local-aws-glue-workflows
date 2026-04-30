"""In-memory representation of an AWS Glue Workflow.

Names mirror the public AWS Glue API (Workflow, Trigger, Predicate, Condition, Action).
This is the contract between the parser and the translator.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from glue_airflow_local.exceptions import InvalidWorkflowError


class TriggerType(str, Enum):
    ON_DEMAND = "ON_DEMAND"
    CONDITIONAL = "CONDITIONAL"
    SCHEDULED = "SCHEDULED"


class JobState(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    STOPPED = "STOPPED"


class LogicalOperator(str, Enum):
    AND = "AND"
    OR = "OR"


@dataclass(frozen=True)
class Job:
    """A Glue ETL job: name plus the location of its PySpark script."""

    name: str
    script_location: str


@dataclass(frozen=True)
class Action:
    """Single action in a trigger -- for now, only job actions (crawlers deferred)."""

    job_name: str


@dataclass(frozen=True)
class Condition:
    """Single predicate condition: job X reached state Y."""

    job_name: str
    state: JobState = JobState.SUCCEEDED


@dataclass(frozen=True)
class Predicate:
    """Boolean expression over job states; controls when a CONDITIONAL trigger fires."""

    conditions: tuple[Condition, ...]
    logical: LogicalOperator = LogicalOperator.AND

    def __post_init__(self) -> None:
        if not self.conditions:
            raise InvalidWorkflowError("Predicate must contain at least one condition")


@dataclass
class Trigger:
    """A Glue workflow trigger. Fires actions when its predicate or schedule is satisfied."""

    name: str
    type: TriggerType
    actions: list[Action]
    predicate: Predicate | None = None
    schedule: str | None = None  # cron expression for SCHEDULED triggers

    def __post_init__(self) -> None:
        if not self.actions:
            raise InvalidWorkflowError(f"Trigger {self.name!r} has no actions")
        if self.type is TriggerType.ON_DEMAND and self.predicate is not None:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: ON_DEMAND triggers must not have a predicate"
            )
        if self.type is TriggerType.ON_DEMAND and self.schedule is not None:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: ON_DEMAND triggers must not have a schedule"
            )
        if self.type is TriggerType.CONDITIONAL and self.predicate is None:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: CONDITIONAL triggers must have a predicate"
            )
        if self.type is TriggerType.CONDITIONAL and self.schedule is not None:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: CONDITIONAL triggers must not have a schedule"
            )
        if self.type is TriggerType.SCHEDULED and not self.schedule:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: SCHEDULED triggers must have a schedule"
            )
        if self.type is TriggerType.SCHEDULED and self.predicate is not None:
            raise InvalidWorkflowError(
                f"Trigger {self.name!r}: SCHEDULED triggers must not have a predicate"
            )


@dataclass
class Workflow:
    """A Glue workflow: a set of triggers wiring jobs into a runnable graph."""

    name: str
    triggers: list[Trigger]
    jobs: dict[str, Job] = field(default_factory=dict)
    description: str | None = None
