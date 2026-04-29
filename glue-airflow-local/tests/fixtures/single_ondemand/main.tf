resource "aws_glue_workflow" "etl" {
  name        = "etl-workflow"
  description = "Single on-demand trigger workflow"
}

resource "aws_glue_job" "extract" {
  name     = "extract-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command {
    script_location = "s3://scripts/extract.py"
  }
}

resource "aws_glue_trigger" "start" {
  name          = "start-trigger"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl.name

  actions {
    job_name = aws_glue_job.extract.name
  }
}
