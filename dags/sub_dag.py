from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator
from datetime import datetime, timedelta
from subdag.subdag_factory import subdag_factory

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@task.python(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = 'netflix'
    partner_path = '/path'
    return {'partner_name': partner_name, 'partner_path': partner_path}

@dag(
    description='This is my XCOM DAG',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
)
def sub_dag():
    
    process_tasks = SubDagOperator(
        task_id='process_tasks',
        subdag=subdag_factory("sub_dag", "process_tasks", default_args),
    )
    
    extract() >> process_tasks
    
dag = sub_dag()
