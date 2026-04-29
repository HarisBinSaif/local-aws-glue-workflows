resource "aws_glue_workflow" "etl" {
  name = "linear-etl"
}

resource "aws_glue_job" "extract" {
  name     = "extract-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/extract.py" }
}

resource "aws_glue_job" "transform" {
  name     = "transform-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/transform.py" }
}

resource "aws_glue_job" "load" {
  name     = "load-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/load.py" }
}

resource "aws_glue_trigger" "start" {
  name          = "start"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl.name

  actions { job_name = aws_glue_job.extract.name }
}

resource "aws_glue_trigger" "after_extract" {
  name          = "after-extract"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl.name

  predicate {
    conditions {
      job_name = aws_glue_job.extract.name
      state    = "SUCCEEDED"
    }
  }

  actions { job_name = aws_glue_job.transform.name }
}

resource "aws_glue_trigger" "after_transform" {
  name          = "after-transform"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl.name

  predicate {
    conditions {
      job_name = aws_glue_job.transform.name
      state    = "SUCCEEDED"
    }
  }

  actions { job_name = aws_glue_job.load.name }
}
