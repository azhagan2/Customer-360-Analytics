provider "aws" {
  region = var.region
}

#########################
# Variables
#########################
variable "region" {}

variable "bucket_name" {}
variable "glue_role" {}
variable "glue_output_database_name" {}
variable "glue_silver_database_name" {}
variable "glue_input_database_name" {}

variable "S3_TARGET_PATH" {}
variable "S3_SILVER_TARGET_PATH" {}
variable "glue_release_files" {}

#########################
# IAM Role for Glue
#########################
resource "aws_iam_role" "glue_role" {
  name = var.glue_role

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_console_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}
resource "aws_iam_role_policy_attachment" "glue_cloudwatch_logs" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonCloudWatchEvidentlyFullAccess"
}



#########################
# Glue Jobs
#########################
locals {
  glue_jobs = {
    "pre_processing"          = "pre_processing.py"
    "purchase_behavior"       = "purchase_behavior.py"
    "churn_prediction"        = "churn_prediction.py"
    "omni_channel_engagement" = "omni_channel_engagement.py"
    "fraud_detection"         = "fraud_detection.py"
    "pricing_trends"          = "pricing_trends.py"
  }
}

resource "aws_glue_job" "jobs" {
  for_each = local.glue_jobs

  name     = each.key
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/code/customer_analytics/${each.value}"
    python_version  = "3"
  }

  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--job-bookmark-option"     = "job-bookmark-enable"
    "--TempDir"                 = "s3://${var.bucket_name}/code/temp/${each.key}/"
    "--INPUT_DB"                = var.glue_input_database_name
    "--extra-py-files"          = var.glue_release_files
    "--job-language"            = "python"

    "--INPUT_DB" = each.key == "pre_processing" ? var.glue_input_database_name : var.glue_silver_database_name
    "--OUTPUT_DB" = each.key == "pre_processing" ? var.glue_silver_database_name : var.glue_output_database_name

    # Conditional target path
    "--S3_TARGET_PATH" = each.key == "pre_processing" ? var.S3_SILVER_TARGET_PATH : var.S3_TARGET_PATH
  }

  worker_type        = "G.1X"
  number_of_workers  = 2
}


#########################
# Glue Crawler
#########################
resource "aws_glue_crawler" "crawler" {
  name          = "customer360_crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = var.glue_output_database_name

  # Crawl Gold Layer
  s3_target {
    path = var.S3_TARGET_PATH
  }

  schema_change_policy {
    update_behavior = "LOG"
    delete_behavior = "LOG"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  
}



resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionsGlueOrchestrationRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name = "stepfunction-glue-access"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:StartCrawler",
          "glue:GetCrawler"
        ],
        Resource = "*"
      }
    ]
  })
}


resource "aws_sfn_state_machine" "glue_orchestration" {
  name     = "GlueETLOrchestration"
  role_arn = aws_iam_role.step_function_role.arn
  definition = file("${path.module}/glue_step_function.json")
}