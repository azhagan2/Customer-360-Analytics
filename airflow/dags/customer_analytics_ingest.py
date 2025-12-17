from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

with DAG(
    dag_id="glue_jobs_and_crawler_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "glue", "s3", "etl"]
) as dag:

    # Job 1: RDS → S3
    rds_to_s3 = AwsGlueJobOperator(
        task_id="rds_to_s3_job",
        job_name="rds-to-s3-job",
        region_name="ap-southeast-2",      # change if needed
        aws_conn_id="aws_stage",       # Airflow AWS connection name
        script_args={"--env": "stage"} # optional parameters
    )

    # Job 2: DynamoDB → S3
    dynamodb_to_s3 = AwsGlueJobOperator(
        task_id="dynamodb_to_s3_job",
        job_name="dynamodb-to-s3-job",
        region_name="ap-southeast-2",
        aws_conn_id="aws_stage",
    )

    # Start crawler
    run_crawler = AwsGlueCrawlerOperator(
        task_id="s3_customer_analytics_crawler",
        config={"Name": "s3_customer_analytics_crawler"},
        aws_conn_id="aws_stage",
    )

    # Dependencies
    rds_to_s3 >> dynamodb_to_s3 >> run_crawler
