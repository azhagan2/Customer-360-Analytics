from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import time
from botocore.exceptions import ClientError

# ====== CONFIG ======
ATHENA_DATABASE = "customer_analytics_db_silver"
ATHENA_OUTPUT = "s3://customer-analytics-july2025/athena/queryResults/"


QUERY_STRING = """
SELECT * FROM "customer_analytics_db_bronze"."customers" limit 10;
"""




RECEIVER_EMAIL = "velmuruganms12@gmail.com"

SENDER_EMAIL = "velms2024@gmail.com"
REGION = "ap-southeast-1"




# ====== FUNCTIONS ======

def run_athena_query(**kwargs):
    athena = boto3.client('athena', region_name=REGION)
    response = athena.start_query_execution(
        QueryString=QUERY_STRING,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed with state: {state}")

    kwargs['ti'].xcom_push(key='query_execution_id', value=query_execution_id)





def get_results_and_send_email(**kwargs):
    query_execution_id = kwargs['ti'].xcom_pull(key='query_execution_id')

    athena = boto3.client('athena', region_name=REGION)
    paginator = athena.get_paginator('get_query_results')
    results_iter = paginator.paginate(QueryExecutionId=query_execution_id)

    rows = []
    for page in results_iter:
        for row in page['ResultSet']['Rows']:
            rows.append([col.get('VarCharValue', '') for col in row['Data']])

    headers = rows[0]
    data_rows = rows[1:]

    table_html = """
    <html>
    <head>
      <style>
        table {{ border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
      </style>
    </head>
    <body>
      <h2>📊 Sales Supplier Summary   334343434</h2>
      <table>
        <tr>{}</tr>
    """.format("".join(f"<th>{h}</th>" for h in headers))

    for row in data_rows:
        table_html += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"

    table_html += "</table></body></html>"

    # Send email
    ses = boto3.client('ses', region_name=REGION)
    try:

        print(table_html)
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={'ToAddresses': [RECEIVER_EMAIL]},
            Message={
                'Subject': {'Data': 'Daily Sales Supplier Summary Report   21111'},
                'Body': {'Html': {'Data': table_html}}
            }
        )
        print("✅ Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("❌ Email failed:", e.response['Error']['Message'])

# ====== DAG DEFINITION ======

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_athena_sales_supplier_report',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run Athena query daily and send email report via SES'
) as dag:

    run_query = PythonOperator(
        task_id='run_athena_query',
        python_callable=run_athena_query,
        provide_context=True
    )

    send_email = PythonOperator(
        task_id='get_results_and_send_email',
        python_callable=get_results_and_send_email,
        provide_context=True
    )

    run_query >> send_email
