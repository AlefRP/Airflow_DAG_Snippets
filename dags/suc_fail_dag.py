from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks
from airflow.operators.empty import EmptyOperator
from airflow.sensors.date_time import DateTimeSensor

# @task.python(task_id='extract_partners', do_xcom_push=False, multiple_outputs=True)
# def extract(partner_name, partner_path):
#     return {'partner_name': partner_name, 'partner_path': partner_path}
   
default_args = {
    'start_date': datetime(2021, 1, 1),
    'retries': 3
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

def _success_callback(context):
    print(context)
    
def _failure_callback(context):
    print(context)

def _extract_callback_success(context):
    print(f"Success: {context}")
    
def _extract_callback_failure(context):
    from airflow.exceptions import AirflowException
    if (context['exception']):
        if (isinstance(context['exception'], AirflowException)):
            print("Error: {}".format(context['exception']))
    print(f"Failure: {context}")
    
def _extracct_callback_retry(context):
    from airflow.exceptions import AirflowFailException
    if (context['ti'].try_number() > 2):
        raise AirflowFailException("Failed 2 times")
    print(f"Retry: {context}")
    
def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS: {dag}, {task_list}, {blocking_task_list}, {slas}, {blocking_tis}")

@dag(
    description='This is my Task G DAG',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    sla_miss_callback=_sla_miss_callback, # Callback function when the SLA is missed
    on_success_callback=_success_callback,
    on_failure_callback=_failure_callback
    # concurrency=2, # Maximo duas tarefas(taks) executando ao mesmo tempo
    # max_active_runs=1 # Maximo de um dag_run por vez
)
def suc_fail_dag():
    
    start = EmptyOperator(
        task_id='start', 
        trigger_rule='dummy', 
        task_concurrency=1, # Essa task não executa mais de uma vez em todas as dag runs
        execution_timeout=timedelta(minutes=5) # Timeout after 5 minutes
        ) 

    delay = DateTimeSensor(
        task_id='delay',
        target_time='{{ execution_date.add(hours=9) }}',
        poke_interval=60 * 60, # Checks if the target time is reached every hour
        mode='reschedule', # Standard: 'poke', with the option to 'reschedule', the worker is used just when its time to check
        timeout=60 * 60 * 10, # Timeout after 10 hours (Default 7 days) - always define it
        execution_timeout=timedelta(minutes=6),
        soft_fail=True, # If the SENSOR TIMES OUT, it will skkip the task, not fail
        exponential_backoff=True # Auments the wait time between checks
    )
    
    """
    mode:
    - reschedule: Reschedule the task when the target time is reached
    - fail: Fail the task when the target time is reached
    - poke: Continuously check if the target time is reached
    """
    
    for _, details in partners.items():
        
        @task.python(
            task_id=f'extract_{details["name"]}',
            sla=timedelta(minutes=15), # SLA: Service Level Agreement, the maximum time the task should take to run (não faz a tarefa falhar, é só um alerta)
            retries=2, # Retry 2 times
            retry_delay=timedelta(seconds=30), # Wait 30 seconds between retries
            retry_exponential_backoff=True, # Exponential backoff between retries, each retry will get longer, for an API ou database consuming task for example
            max_retry_delay=timedelta(minutes=15), # Max retry delay between retries, if the task is still failing after this time, it will fail
            on_success_callback=_extract_callback_success,
            on_failure_callback=_extract_callback_failure,
            on_retry_callback=_extracct_callback_retry,
            priority_weight=details['priority'], 
            pool_slots=1, do_xcom_push=False, 
            multiple_outputs=True
            )
        def extract(partner_name, partner_path):
            return {'partner_name': partner_name, 'partner_path': partner_path}

        extract_task = extract(details['name'], details['path'])
        
        start >> extract_task >> delay

        # Add unique group_id based on partner name
        process_group = process_tasks.override(group_id=f'process_tasks_{details["name"]}')
        process_group(partner_settings=extract_task)

dag = suc_fail_dag()