from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks
from airflow.operators.empty import EmptyOperator

# @task.python(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)
# def extract(partner_name, partner_path):
#     return {'partner_name': partner_name, 'partner_path': partner_path}
   
default_args = {
    'start_date': datetime(2021, 1, 1)
}

partners = {
    'partner_snowflake': {
        'name': 'snowflake',
        'path': '/path/to/snowflake',
        'priority': 1
    },
    'partner_netflix': {
        'name': 'netflix',
        'path': '/path/to/netflix',
        'priority': 2
    },
    'partner_astronomer': {
        'name': 'astronomer',
        'path': '/path/to/astronomer',
        'priority': 3
    }
}

@dag(
    description='This is my Task G DAG',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    concurrency=2, # Maximo duas tarefas(taks) executando ao mesmo tempo todos os dag runs
    max_active_runs=1 # Maximo de um dag_run por vez
)
def dy_tasks():
    
    start = EmptyOperator(task_id='start', trigger_rule='dummy', task_concurrency=1) # Essa task não executa mais de uma vez em todas as dag runs

    for _, details in partners.items():
        
        @task.python(task_id=f'extract_{details["name"]}', priority_weight=details['priority'], pool_slots=1, do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {'partner_name': partner_name, 'partner_path': partner_path}

        extract_task = extract(details['name'], details['path'])
        
        start >> extract_task

        # Add unique group_id based on partner name
        process_group = process_tasks.override(group_id=f'process_tasks_{details["name"]}')
        process_group(partner_settings=extract_task)

dag = dy_tasks()