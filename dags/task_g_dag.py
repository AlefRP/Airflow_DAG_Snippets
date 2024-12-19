from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks

@task.python(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = 'netflix'
    partner_path = '/path'
    return {'partner_name': partner_name, 'partner_path': partner_path}
   
default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag(
    description='This is my Task G DAG',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
)
def task_g_dag():
    partner_settings = extract()
    process_tasks(partner_settings)

dag = task_g_dag()