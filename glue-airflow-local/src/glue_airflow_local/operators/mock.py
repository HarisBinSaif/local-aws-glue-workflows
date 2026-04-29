"""Mock Glue job operator — the default executor for v0.1.

Runs nothing real: loads default_params.json from the workflow directory,
merges DAG-run conf on top, and either succeeds, fails, or sleeps.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

_LOG = logging.getLogger(__name__)
_VALID_BEHAVIORS = frozenset({"succeed", "fail", "sleep"})


class MockGlueJobOperator(BaseOperator):
    """Pretend to run a Glue job. Exists so users can iterate on workflow shape."""

    template_fields = ("job_name", "workflow_dir")

    def __init__(
        self,
        *,
        job_name: str,
        workflow_dir: str,
        behavior: str = "succeed",
        sleep_seconds: float = 0.0,
        **kwargs: Any,
    ) -> None:
        if behavior not in _VALID_BEHAVIORS:
            raise ValueError(
                f"Unknown behavior {behavior!r}; expected one of {sorted(_VALID_BEHAVIORS)}"
            )
        super().__init__(**kwargs)
        self.job_name = job_name
        self.workflow_dir = workflow_dir
        self.behavior = behavior
        self.sleep_seconds = sleep_seconds

    def execute(self, context: Context) -> dict[str, Any]:
        params = self._merged_params(context)
        _LOG.info(
            "MockGlueJobOperator running job=%s behavior=%s param_keys=%s",
            self.job_name,
            self.behavior,
            sorted(params.keys()),
        )
        if self.behavior == "fail":
            raise RuntimeError(
                f"MockGlueJobOperator: job {self.job_name!r} configured to fail"
            )
        if self.behavior == "sleep":
            time.sleep(self.sleep_seconds)
        return {"job_name": self.job_name, "status": "SUCCEEDED", "params": params}

    def _merged_params(self, context: Context) -> dict[str, Any]:
        defaults_path = Path(self.workflow_dir) / "default_params.json"
        defaults: dict[str, Any] = {}
        if defaults_path.is_file():
            defaults = json.loads(defaults_path.read_text())

        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        return {**defaults, **conf}
