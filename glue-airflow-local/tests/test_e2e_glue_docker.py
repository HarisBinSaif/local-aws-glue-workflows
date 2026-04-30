"""End-to-end smoke test for the Plan B docker stack.

Skipped by default. Run explicitly:

    pytest -m docker_e2e -v

Pre-requisites:
- ``docker compose -f docker/docker-compose.yaml up -d`` is running.
- The `glue-runner`, `minio`, and `airflow-*` services are healthy.

What this exercises:
1. Generate the simple-etl DAG with --executor=glue-docker.
2. Copy it into the airflow-dags volume.
3. Trigger the DAG via Airflow CLI (inside the scheduler container).
4. Wait for terminal state.
5. Assert the final Parquet shows up in MinIO's local-output bucket.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_DIR = REPO_ROOT / "examples" / "simple-etl"
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "docker" / "docker-compose.yaml")]


pytestmark = pytest.mark.docker_e2e


def _stack_running() -> bool:
    try:
        result = subprocess.run(
            [*COMPOSE, "ps", "--format", "json"],
            capture_output=True, text=True, check=False, timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0 and "glue-runner" in result.stdout


@pytest.fixture(scope="module", autouse=True)
def _require_stack() -> None:
    if not _stack_running():
        pytest.skip(
            "docker stack is not running; "
            "start it with `docker compose -f docker/docker-compose.yaml up -d`"
        )


def _run(cmd: list[str], **kw: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=True, **kw)  # type: ignore[arg-type]


def test_simple_etl_runs_end_to_end(tmp_path: Path) -> None:
    # 1. Generate the DAG file.
    dag_path = tmp_path / "simple_etl_dag.py"
    _run([
        "glue-airflow-local", "translate", str(EXAMPLE_DIR / "terraform"),
        "--output", str(dag_path),
        "--workflow-dir", str(EXAMPLE_DIR),
        "--executor", "glue-docker",
    ])
    assert dag_path.is_file()
    assert "GlueDockerOperator" in dag_path.read_text()

    # 2. Copy it into the scheduler's dags volume.
    scheduler_id = _run([*COMPOSE, "ps", "-q", "airflow-scheduler"]).stdout.strip()
    _run(["docker", "cp", str(dag_path), f"{scheduler_id}:/opt/airflow/dags/simple_etl_dag.py"])

    # 3. Wait for Airflow to parse the DAG file, then force serialization into the
    #    DagModel table (otherwise `dags trigger` fails with DagNotFound on a fresh
    #    file the scheduler has not yet processed in its periodic loop).
    deadline = time.time() + 60
    while time.time() < deadline:
        ls = _run([*COMPOSE, "exec", "-T", "airflow-scheduler", "airflow", "dags", "list"])
        if "simple-etl" in ls.stdout:
            break
        time.sleep(2)
    else:
        pytest.fail("Airflow never registered the simple-etl DAG")

    _run([
        *COMPOSE,
        "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "reserialize",
    ])

    # New DAGs default to paused; the scheduler will not run a paused DAG.
    _run([
        *COMPOSE,
        "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "unpause", "simple-etl",
    ])

    # The generated DAG hardcodes ``workflow_dir`` to a path on the developer's host
    # machine, which is not visible inside the airflow-scheduler container. The
    # operator reads ``default_params.json`` from there and merges it with
    # ``dag_run.conf``. Inside the container the file is missing, so we pass the
    # bucket params via ``--conf`` to satisfy the PySpark scripts'
    # ``getResolvedOptions(...)`` calls.
    conf = (REPO_ROOT / "examples" / "simple-etl" / "default_params.json").read_text()

    run_id = f"e2e-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    _run([
        *COMPOSE,
        "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "trigger", "simple-etl",
        "--run-id", run_id,
        "--conf", conf,
    ])

    # 4. Wait for terminal state (success or failure), max ~5 min for first Spark warm-up.
    #    `airflow dags state` requires execution_date (not run_id), so we use
    #    `dags list-runs` and filter by run_id ourselves.
    state = ""
    deadline = time.time() + 300
    while time.time() < deadline:
        runs_json = _run([
            *COMPOSE,
            "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs",
            "--dag-id", "simple-etl",
            "--output", "json",
        ]).stdout
        try:
            runs = json.loads(runs_json)
        except json.JSONDecodeError:
            runs = []
        match = next((r for r in runs if r.get("run_id") == run_id), None)
        if match is not None:
            state = (match.get("state") or "").strip()
            if state in ("success", "failed"):
                break
        time.sleep(5)
    else:
        pytest.fail(f"DAG run {run_id} did not finish within 5 minutes (last state: {state!r})")

    assert state == "success", f"DAG run {run_id} ended in state: {state}"

    # 5. Assert the final Parquet exists in MinIO.
    # The minio/minio server image has no `mc`; spin up a one-shot mc container
    # attached to the same compose network so it can reach the `minio` service by name.
    network = _compose_network()
    listing = _run([
        "docker", "run", "--rm",
        "--network", network,
        "--entrypoint", "sh",
        "minio/mc:latest", "-c",
        "mc alias set local http://minio:9000 minio minio123 >/dev/null && "
        "mc ls --recursive local/local-output/department-summary",
    ])
    assert ".parquet" in listing.stdout, listing.stdout


def _compose_network() -> str:
    """Return the docker network name created by the compose stack.

    Compose v2 names the default network ``<project>_default``. The project name
    defaults to the directory containing the compose file (``docker``), so it is
    typically ``docker_default``. We discover it dynamically so the test does not
    break if the user overrides the compose project name.
    """
    result = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        capture_output=True, text=True, check=True, timeout=10,
    )
    candidates = [n for n in result.stdout.split() if n.endswith("_default")]
    # Prefer a network that has the `minio` container attached.
    for name in candidates:
        inspect = subprocess.run(
            ["docker", "network", "inspect", name, "--format",
             "{{range .Containers}}{{.Name}} {{end}}"],
            capture_output=True, text=True, check=False, timeout=10,
        )
        if "minio" in inspect.stdout:
            return name
    if candidates:
        return candidates[0]
    pytest.fail("Could not find a docker compose network ending in _default")
