from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='bash_op',
    description='This is my first DAG using BashOperator',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'analytics'],
    catchup=False,
) as dag:

    create_tables_task = BashOperator(
        task_id='create_tables_task',
        bash_command='cd /usr/local/airflow/dags/scripts && /home/astro/sqla/bin/python create_tables.py',
    )

    create_tables_task