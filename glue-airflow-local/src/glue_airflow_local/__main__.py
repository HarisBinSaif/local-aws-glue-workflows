"""Allow `python -m glue_airflow_local ...` invocation."""

from glue_airflow_local.cli import main

raise SystemExit(main())
