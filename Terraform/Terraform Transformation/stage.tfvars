bucket_name         = "customer360-az"
glue_role           = "GlueDevRoleCA"


glue_input_database_name  = "customer_analytics_db_bronze"
glue_silver_database_name  = "customer_analytics_db_silver_az"
glue_output_database_name  = "customer_analytics_db_gold_az"


region="us-east-1"


S3_SILVER_TARGET_PATH="s3://customer360-az/analytics/silver/"
S3_TARGET_PATH="s3://customer360-az/analytics/gold/"


glue_release_files      = "s3://customer360-az/code/customer_analytics/customer_analytics-3.0.1-py3-none-any.whl"

# c:\Users\Azhagan\Desktop\AWS Data Engineering\dev_morning_batch\jupyter_workspace\customer360_analytics\dist\customer_analytics-2.2.0-py3-none-any.whl