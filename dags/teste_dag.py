from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from groups.process_tasks_b import process_tasks
import time

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

default_args = {
    'start_date': datetime(2021, 1, 1)
}

@dag(
    description='Testes que desjo fazer',
    default_args=default_args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
)
def teste_dag():
    
    start = EmptyOperator(task_id='start')
    
    # choosing_partner_based_on_day = BranchPythonOperator(
    # task_id='choosing_partner_based_on_day',
    # python_callable=lambda execution_date: (
    #     'extract_astronomer' if execution_date.isoweekday() == 5 else  # Sexta-feira
    #     'extract_snowflake' if execution_date.isoweekday() == 1 else  # Segunda-feira
    #     'extract_netflix' if execution_date.isoweekday() == 3 else  # Quarta-feira
    #     'stop'
    #     )
    # )
    
    storing = EmptyOperator(task_id='storing', trigger_rule='none_failed_or_skipped')
    
    cleaning_db = TriggerDagRunOperator(
        task_id='cleaning_db',
        trigger_dag_id='cleaning_db_dag',
        execution_date='{{ ds }}',
        wait_for_completion=True, # Espera a dag ser executada para passar para próxima task
        poke_interval=20, # Checa se a dag foi executada com sucesso a cada 20 segundos (Padrão 60)
        reset_dag_run=True, # Se a dag já foi executada, reinicia a dag, assim posso reexecula para mesmo excetution_date (para rodar novamente sem erros)
        failed_states=['failed']
    )
    
    stop = EmptyOperator(task_id='stop')
    
    storing >> cleaning_db >> stop
    
    for _, details in partners.items():
         # Com depend_on_past=True, a task vai ser executada apenas se a mesma task na execução anterior não falhou
         # Com wait_for_downstream=True, a task vai ser executada apenas se a mesma task na execução anterior e a task acima dela nao falhou (sucessed ou skkiped)
         # Com wait_for_downstream=True e depend_on_past já é setado para True
        @task.python(task_id=f'extract_{details["name"]}', depends_on_past=True, wait_for_downstream=True, pool='partner_pool', pool_slots=1, multiple_outputs=True)
        def extract(partner_name, partner_path):
            time.sleep(3)
            return {'partner_name': partner_name, 'partner_path': partner_path}
        
        extracted_values = extract(details['name'], details['path'])
        
        start  >> extracted_values
        
        process_tasks(extracted_values, f'process_{details["name"]}') >> storing
    
dag = teste_dag()
