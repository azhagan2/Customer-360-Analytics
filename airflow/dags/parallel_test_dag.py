from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import random

def test_task(task_name):
    print(f"Task {task_name} started.")
    sleep_time = random.randint(10, 20)
    print(f"{task_name} sleeping for {sleep_time} seconds...")
    time.sleep(sleep_time)
    print(f"Task {task_name} completed.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 21),
}

with DAG(
    dag_id='parallel_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_tasks=10,    # replaces dag_concurrency
    max_active_runs=1,      # replaces max_active_runs_per_dag
    concurrency=10,         # optional safeguard for compatibility
) as dag:

    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print("Starting DAG...")
    )

    task1 = PythonOperator(
        task_id='parallel_task_1',
        python_callable=test_task,
        op_kwargs={'task_name': 'Task 1'}
    )

    task2 = PythonOperator(
        task_id='parallel_task_2',
        python_callable=test_task,
        op_kwargs={'task_name': 'Task 2'}
    )

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print("All done!")
    )

    # Run both tasks in parallel between start and end
    start >> [task1, task2] >> end
