from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator


def  writeComplete():
    print("   any server    ----  write to snowflake completed")





# Define the SQL query to execute in Snowflake
SNOWFLAKE_SQL = """
  COPY INTO @demo.demo_schema.AWS_DEMO_STAGE2/customer_data_2
FROM (
  SELECT *  FROM snowflake_sample_data.TPCH_SF1.customer
)
INCLUDE_QUERY_ID = TRUE;

"""

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

# Define the DAG
with DAG(
    dag_id='snow_demo1',
    default_args=default_args,
    description='A simple DAG to run Snowflake SQL',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Snowflake task
    run_snowflake_sql = SnowflakeOperator(
        task_id='snowflake_sql',
        snowflake_conn_id='snowflakeid',  # Connection ID
        sql=SNOWFLAKE_SQL,  # SQL query to execute
    )

        # Task 1: Run a Python function
    task1 = PythonOperator(
        task_id='print_task_1',
        python_callable=writeComplete,
    )



    run_snowflake_sql  >> task1
