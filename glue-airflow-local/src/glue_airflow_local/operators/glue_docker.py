"""GlueDockerOperator — runs a real PySpark script inside a long-running Glue container.

The operator strips the ``s3://<bucket>/`` prefix from the Terraform ``script_location``
and looks for the rest under a host-mounted ``/scripts/`` path inside the container.
Job parameters (default_params.json + DAG-run conf) are passed to the script as
``--KEY=VALUE`` arguments to ``spark-submit``; the user's ``getResolvedOptions(...)`` call
reads them as it would in real Glue.

S3 calls inside the container hit MinIO via the ``AWS_ENDPOINT_URL`` env var set on the
container itself (see ``docker/docker-compose.yaml``), not by this operator.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import docker
from airflow.models import BaseOperator
from airflow.utils.context import Context

_LOG = logging.getLogger(__name__)
_S3_PREFIX = "s3://"
_DEFAULT_CONTAINER = "glue-runner"
_DEFAULT_MOUNT = "/scripts"


def _resolve_script_path(script_location: str, mount: str) -> str:
    """Strip ``s3://<bucket>/`` and prepend the in-container mount point.

    Example::

        _resolve_script_path("s3://example-bucket/scripts/extract.py", "/scripts")
        -> "/scripts/scripts/extract.py"
    """
    if not script_location.startswith(_S3_PREFIX):
        raise ValueError(
            f"script_location must start with s3://; got {script_location!r}"
        )
    rest = script_location[len(_S3_PREFIX):]
    # Strip the bucket name (the first path segment).
    _, _, key = rest.partition("/")
    return f"{mount.rstrip('/')}/{key}"


def _build_spark_submit_argv(
    *,
    script_path: str,
    job_name: str,
    workflow_run_id: str,
    params: dict[str, Any],
) -> list[str]:
    """Build the ``spark-submit`` argv. Param order is sorted for reproducibility."""
    argv = ["spark-submit", script_path]
    argv.append(f"--JOB_NAME={job_name}")
    argv.append(f"--WORKFLOW_RUN_ID={workflow_run_id}")
    for key in sorted(params):
        argv.append(f"--{key}={params[key]}")
    return argv


class GlueDockerOperator(BaseOperator):
    """Run a PySpark Glue job inside the long-running Glue Docker container."""

    template_fields = ("job_name", "script_location", "workflow_dir")

    def __init__(
        self,
        *,
        job_name: str,
        script_location: str,
        workflow_dir: str,
        container_name: str = _DEFAULT_CONTAINER,
        script_mount: str = _DEFAULT_MOUNT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.script_location = script_location
        self.workflow_dir = workflow_dir
        self.container_name = container_name
        self.script_mount = script_mount

    def execute(self, context: Context) -> dict[str, Any]:
        params = self._merged_params(context)
        script_path = _resolve_script_path(self.script_location, self.script_mount)
        argv = _build_spark_submit_argv(
            script_path=script_path,
            job_name=self.job_name,
            workflow_run_id=str(context.get("ds_nodash", "")),
            params=params,
        )
        _LOG.info(
            "GlueDockerOperator job=%s container=%s argv=%s",
            self.job_name,
            self.container_name,
            argv,
        )
        client = docker.from_env()
        container = client.containers.get(self.container_name)
        result = container.exec_run(argv, demux=False)
        output = result.output.decode("utf-8", errors="replace") if result.output else ""
        for line in output.splitlines():
            _LOG.info("[%s] %s", self.job_name, line)
        if result.exit_code != 0:
            raise RuntimeError(
                f"GlueDockerOperator: job {self.job_name!r} failed with exit code "
                f"{result.exit_code}. Last lines:\n{output[-2000:]}"
            )
        return {
            "job_name": self.job_name,
            "status": "SUCCEEDED",
            "exit_code": result.exit_code,
        }

    def _merged_params(self, context: Context) -> dict[str, Any]:
        defaults_path = Path(self.workflow_dir) / "default_params.json"
        defaults: dict[str, Any] = {}
        if defaults_path.is_file():
            defaults = json.loads(defaults_path.read_text())
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        return {**defaults, **conf}
