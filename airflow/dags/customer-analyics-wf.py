from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import time
from datetime import datetime, timedelta
import boto3
import time
from botocore.exceptions import ClientError


# Helper function to start and wait for Glue job
def start_and_wait_glue_job(job_name, **kwargs):
    glue = boto3.client('glue', region_name='us-east-1')  # update region if needed

    # Start job run
    response = glue.start_job_run(JobName=job_name)
    job_run_id = response['JobRunId']
    print(f"Started Glue job {job_name} with Run ID: {job_run_id}")

    # Poll status
    while True:
        status_response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
        status = status_response['JobRun']['JobRunState']
        print(f"Current status of {job_name}: {status}")

        if status in ['SUCCEEDED']:
            print(f"{job_name} completed successfully.")
            break
        elif status in ['FAILED', 'TIMEOUT', 'STOPPED']:
            raise Exception(f"{job_name} failed with status: {status}")
        time.sleep(15)  # wait between polling



# DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 27),
    'retries': 0
}

with DAG('customer_analytics_wf',
         default_args=default_args,
         schedule_interval=None,
         catchup=False,
         tags=['glue', 'aws', 'retail'],
         max_active_runs=1,   # number of DAG runs
         concurrency=10,      # number of active tasks allowed
         description='Run Glue Jobs in Sequence') as dag:
    
    run_pre_processing = PythonOperator(
        task_id='pre_processing',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'pre_processing'},
    )

    run_purchase_behavior = PythonOperator(
        task_id='run_purchase_behavior',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'purchase_behavior'},
    )

    run_churn_prediction = PythonOperator(
        task_id='run_churn_prediction',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'churn_prediction'},
    )


    run_omni_channel_engagement = PythonOperator(
        task_id='omni_channel_engagement',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'omni_channel_engagement'},
    )

    run_fraud_detection = PythonOperator(
        task_id='fraud_detection',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'fraud_detection'},
    )

    run_pricing_trends = PythonOperator(
        task_id='pricing_trends',
        python_callable=start_and_wait_glue_job,
        op_kwargs={'job_name': 'pricing_trends'},
    )


    run_pre_processing >> [run_purchase_behavior, run_churn_prediction]
    [run_purchase_behavior, run_churn_prediction] >> run_omni_channel_engagement

    run_omni_channel_engagement >> run_fraud_detection >> run_pricing_trends