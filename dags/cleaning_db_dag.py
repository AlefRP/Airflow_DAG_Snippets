from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag(
    description='This is my Task G DAG',
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
)
def cleaning_db_dag():
    
    clean_db = EmptyOperator(task_id='clean_db')
    
    clean_db
    
cleaning_db_dag()
    