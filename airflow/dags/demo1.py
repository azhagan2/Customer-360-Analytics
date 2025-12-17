from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# Simple Python function to run inside a task
def hello_world():
    print("starting from Airflow!")


def hello_world2():
    print("Ending workflow")


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id="simple_dag_example",
    default_args=default_args,
    description="A simple Airflow DAG example",
    start_date=datetime(2025, 8, 30),   # REQUIRED
    schedule_interval="@daily",         # Runs daily
    catchup=False                       # Prevent backfilling
) as dag:

    # Define tasks
    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world
    )

    task2 = PythonOperator(
        task_id="print_another",
        python_callable=hello_world2
    )

    # Set task dependencies
    task1 >> task2
