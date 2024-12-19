from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks
from airflow.operators.python import BranchPythonOperator
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
        'path': '/path/to/snowflake'
    },
    'partner_netflix': {
        'name': 'netflix',
        'path': '/path/to/netflix'
    },
    'partner_astronomer': {
        'name': 'astronomer',
        'path': '/path/to/astronomer'
    }
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
def b_dag():
    
    start = EmptyOperator(task_id='start')

    choosing_partner_based_on_day = BranchPythonOperator(
    task_id='choosing_partner_based_on_day',
    python_callable=lambda execution_date: (
        'extract_astronomer' if execution_date.isoweekday() == 5 else  # Sexta-feira
        'extract_snowflake' if execution_date.isoweekday() == 1 else  # Segunda-feira
        'extract_netflix' if execution_date.isoweekday() == 3 else  # Quarta-feira
        'stop'
        )
    )
    
    stop = EmptyOperator(task_id='stop')
    
    """
    Trigger rules:
    - all_success: All upstream tasks succeeded
    - all_failed: All upstream tasks failed
    - all_done: All upstream tasks are done (failed or succeeded)
    - one_failed: At least one upstream task failed
    - one_success: At least one upstream task succeeded
    - none_failed: No upstream tasks failed
    - none_skipped: No upstream tasks were skipped
    - none_failed_or_skipped: No upstream tasks failed or were skipped
    - dummy: Used for testing purposes
    """
    storing = EmptyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')
    
    choosing_partner_based_on_day >> stop
    storing >> stop
    
    for _, details in partners.items():
        
        @task.python(task_id=f'extract_{details["name"]}', do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {'partner_name': partner_name, 'partner_path': partner_path}

        extract_task = extract(details['name'], details['path'])
        
        start >> choosing_partner_based_on_day >> extract_task

        # Add unique group_id based on partner name
        process_group = process_tasks.override(group_id=f'process_tasks_{details["name"]}')
        process_group(partner_settings=extract_task) >> storing

dag = b_dag()