resource "aws_glue_workflow" "fanout" {
  name = "fanout-workflow"
}

resource "aws_glue_job" "src" {
  name     = "source-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/src.py" }
}

resource "aws_glue_job" "branch_a" {
  name     = "branch-a-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/a.py" }
}

resource "aws_glue_job" "branch_b" {
  name     = "branch-b-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/b.py" }
}

resource "aws_glue_job" "merge" {
  name     = "merge-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/merge.py" }
}

resource "aws_glue_trigger" "start" {
  name          = "start"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.fanout.name
  actions { job_name = aws_glue_job.src.name }
}

resource "aws_glue_trigger" "fanout_a" {
  name          = "fanout-a"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fanout.name
  predicate {
    conditions {
      job_name = aws_glue_job.src.name
      state    = "SUCCEEDED"
    }
  }
  actions { job_name = aws_glue_job.branch_a.name }
}

resource "aws_glue_trigger" "fanout_b" {
  name          = "fanout-b"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fanout.name
  predicate {
    conditions {
      job_name = aws_glue_job.src.name
      state    = "SUCCEEDED"
    }
  }
  actions { job_name = aws_glue_job.branch_b.name }
}

resource "aws_glue_trigger" "merge_after" {
  name          = "merge-after"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fanout.name
  predicate {
    logical = "AND"
    conditions {
      job_name = aws_glue_job.branch_a.name
      state    = "SUCCEEDED"
    }
    conditions {
      job_name = aws_glue_job.branch_b.name
      state    = "SUCCEEDED"
    }
  }
  actions { job_name = aws_glue_job.merge.name }
}
