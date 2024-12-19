from airflow.decorators import task, dag
from datetime import datetime, timedelta
# from typing import Dict

"""
@task.python(task_id='extract_partners')
def extract() -> Dict[str, str]:
    partner_name = 'netflix'
    partener_path = '/path'
    return {'partner_name': partner_name, 'partner_path': partener_path} 
"""

@task.python(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = 'netflix'
    partner_path = '/path'
    return {'partner_name': partner_name, 'partner_path': partner_path}

@task.python  
def transform(partner_name, partner_path):
    print(partner_name)
    print(partner_path)

@dag(
    description='This is my XCOM DAG',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
)
def task_f_dag():
    partner_settings = extract()
    # Passa os valores automaticamente para `transform` usando `>>` 
    transform(partner_settings['partner_name'], partner_settings['partner_path'])
    
dag = task_f_dag()
