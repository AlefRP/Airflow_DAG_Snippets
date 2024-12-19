from airflow import DAG
from airflow.decorators import task
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1),
}

with DAG(
    dag_id="dag_snowflake",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    @task.python
    def process():
        print("Processing: /data/snowflake")
    
    process()
