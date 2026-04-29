resource "aws_glue_workflow" "nightly" {
  name = "nightly-etl"
}

resource "aws_glue_job" "ingest" {
  name     = "ingest-job"
  role_arn = "arn:aws:iam::123456789012:role/GlueRole"
  command { script_location = "s3://x/ingest.py" }
}

resource "aws_glue_trigger" "nightly_2am" {
  name          = "nightly-2am"
  type          = "SCHEDULED"
  schedule      = "cron(0 2 * * ? *)"
  workflow_name = aws_glue_workflow.nightly.name

  actions { job_name = aws_glue_job.ingest.name }
}
