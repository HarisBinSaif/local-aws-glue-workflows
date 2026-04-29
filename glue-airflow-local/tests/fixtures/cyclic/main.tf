resource "aws_glue_workflow" "loop" {
  name = "loop"
}

resource "aws_glue_job" "a" {
  name     = "a-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/a.py" }
}

resource "aws_glue_job" "b" {
  name     = "b-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/b.py" }
}

resource "aws_glue_trigger" "after_a" {
  name          = "after-a"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.loop.name
  predicate {
    conditions {
      job_name = aws_glue_job.a.name
      state    = "SUCCEEDED"
    }
  }
  actions { job_name = aws_glue_job.b.name }
}

resource "aws_glue_trigger" "after_b" {
  name          = "after-b"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.loop.name
  predicate {
    conditions {
      job_name = aws_glue_job.b.name
      state    = "SUCCEEDED"
    }
  }
  actions { job_name = aws_glue_job.a.name }
}
