"""Command-line interface."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from glue_airflow_local.parser import parse_directory
from glue_airflow_local.translator import translate_workflow

_LOG = logging.getLogger("glue_airflow_local")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="glue-airflow-local")
    sub = parser.add_subparsers(dest="command", required=True)

    translate = sub.add_parser(
        "translate",
        help="Translate a Terraform directory into an Airflow DAG file.",
    )
    translate.add_argument("tf_dir", type=Path)
    translate.add_argument("--output", type=Path, required=True)
    translate.add_argument(
        "--workflow-dir",
        type=Path,
        default=None,
        help="Directory holding default_params.json (defaults to tf_dir).",
    )
    translate.add_argument(
        "--executor",
        choices=["mock", "glue-docker"],
        default="mock",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command != "translate":  # pragma: no cover - argparse already validates
        parser.error(f"Unknown command: {args.command}")

    workflow_dir = args.workflow_dir or args.tf_dir
    workflows = parse_directory(args.tf_dir)

    if len(workflows) == 1:
        output_paths = [args.output]
    else:
        output_paths = [
            args.output.with_name(f"{args.output.stem}__{wf.name}{args.output.suffix}")
            for wf in workflows
        ]

    for wf, out_path in zip(workflows, output_paths, strict=True):
        source = translate_workflow(wf, workflow_dir=str(workflow_dir), executor=args.executor)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(source)
        _LOG.info(
            "Wrote %s (workflow=%s, jobs=%d, executor=%s)",
            out_path, wf.name, len(wf.jobs), args.executor,
        )

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
