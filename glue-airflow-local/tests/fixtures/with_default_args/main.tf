resource "aws_glue_workflow" "etl" {
  name = "etl-with-args"
  default_run_properties = {
    OUTPUT_BUCKET = "wf-bucket"
    ENV           = "wf"
  }
}

resource "aws_glue_job" "extract" {
  name     = "extract-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/extract.py" }
  default_arguments = {
    "--ENV"          = "job-env"
    "--JOB_LEVEL"    = "yes"
  }
}

resource "aws_glue_job" "transform" {
  name     = "transform-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/transform.py" }
}

resource "aws_glue_trigger" "start" {
  name          = "start"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl.name
  actions {
    job_name = aws_glue_job.extract.name
    arguments = {
      "--ENV"           = "trigger-env"
      "--TRIGGER_LEVEL" = "yes"
    }
  }
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
  actions {
    job_name = aws_glue_job.transform.name
  }
}
