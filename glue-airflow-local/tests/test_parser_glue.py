"""Tests for extracting Glue resources from parsed Terraform into IR."""

from glue_airflow_local.model import TriggerType
from glue_airflow_local.parser import parse_directory
from glue_airflow_local.parser.glue import extract_workflows
from glue_airflow_local.parser.hcl import read_tf_file


def test_single_ondemand_workflow(fixtures_dir):
    parsed = read_tf_file(fixtures_dir / "single_ondemand" / "main.tf")
    workflows = extract_workflows([parsed])

    assert len(workflows) == 1
    wf = workflows[0]
    assert wf.name == "etl-workflow"
    assert wf.description == "Single on-demand trigger workflow"
    assert wf.jobs == {"extract-job"}
    assert len(wf.triggers) == 1
    trig = wf.triggers[0]
    assert trig.name == "start-trigger"
    assert trig.type is TriggerType.ON_DEMAND
    assert [a.job_name for a in trig.actions] == ["extract-job"]
    assert trig.predicate is None


def test_linear_chain_with_conditional_triggers(fixtures_dir):
    parsed = read_tf_file(fixtures_dir / "linear_chain" / "main.tf")
    workflows = extract_workflows([parsed])
    assert len(workflows) == 1
    wf = workflows[0]
    assert wf.jobs == {"extract-job", "transform-job", "load-job"}

    by_name = {t.name: t for t in wf.triggers}
    assert by_name["start"].type is TriggerType.ON_DEMAND

    after_extract = by_name["after-extract"]
    assert after_extract.type is TriggerType.CONDITIONAL
    assert after_extract.predicate is not None
    assert len(after_extract.predicate.conditions) == 1
    cond = after_extract.predicate.conditions[0]
    assert cond.job_name == "extract-job"
    assert cond.state.value == "SUCCEEDED"
    assert [a.job_name for a in after_extract.actions] == ["transform-job"]


def test_scheduled_trigger(fixtures_dir):
    parsed = read_tf_file(fixtures_dir / "scheduled" / "main.tf")
    workflows = extract_workflows([parsed])
    wf = workflows[0]
    assert len(wf.triggers) == 1
    trig = wf.triggers[0]
    assert trig.type is TriggerType.SCHEDULED
    assert trig.schedule == "cron(0 2 * * ? *)"
    assert [a.job_name for a in trig.actions] == ["ingest-job"]


def test_parallel_branches_with_and_predicate(fixtures_dir):
    parsed = read_tf_file(fixtures_dir / "parallel_branches" / "main.tf")
    workflows = extract_workflows([parsed])
    wf = workflows[0]
    assert wf.jobs == {"source-job", "branch-a-job", "branch-b-job", "merge-job"}

    by_name = {t.name: t for t in wf.triggers}
    merge = by_name["merge-after"]
    assert merge.predicate is not None
    job_names_in_pred = {c.job_name for c in merge.predicate.conditions}
    assert job_names_in_pred == {"branch-a-job", "branch-b-job"}


def test_parse_directory_reads_all_tf_files(fixtures_dir):
    workflows = parse_directory(fixtures_dir / "linear_chain")
    assert len(workflows) == 1
    assert workflows[0].name == "linear-etl"
