from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def step1():
    print("Hello, Airflow !")


def step2():
    print(" Welcome to airflow !")   

def step3():
    print(" Thank you for coming Airflow !")   


with DAG(   dag_id='demo_dag_v3',
    start_date=datetime(2025,11,1),
    #schedule_interval='0 10 * * *',  # Runs daily at 10:00 AM
    #schedule_interval='0 10,14,18 * * *',
    schedule_interval="@daily",  # Runs daily
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='step1',
        python_callable=step1,
    )   

    task2 = PythonOperator(
        task_id='step2',
        python_callable=step2,
    )

    task3 = PythonOperator(
        task_id='step3',
        python_callable=step3,
    )

    [task1 , task3] >> task2
    #[task1, task2] >> [task3, task4]