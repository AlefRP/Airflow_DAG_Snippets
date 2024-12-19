from airflow.models import DAG
from airflow.operators.python import get_current_context
from airflow.decorators import task

@task.python  
def process_a():
    ti =get_current_context()['ti']
    partner_name = ti.xcom_pull(task_ids='extract_partners', key='partner_name', dag_id='sub_dag')
    partner_path = ti.xcom_pull(task_ids='extract_partners', key='partner_path', dag_id='sub_dag')
    print(partner_name)
    print(partner_path)

@task.python  
def process_b():
    ti =get_current_context()['ti']
    partner_name = ti.xcom_pull(task_ids='extract_partners', key='partner_name', dag_id='sub_dag')
    partner_path = ti.xcom_pull(task_ids='extract_partners', key='partner_path', dag_id='sub_dag')
    print(partner_name)
    print(partner_path)
    
@task.python  
def process_c():
    ti =get_current_context()['ti']
    partner_name = ti.xcom_pull(task_ids='extract_partners', key='partner_name', dag_id='sub_dag')
    partner_path = ti.xcom_pull(task_ids='extract_partners', key='partner_path', dag_id='sub_dag')
    print(partner_name)
    print(partner_path)
    
def subdag_factory(parent_dag_id, subdag_id, default_args):
    with DAG(
        dag_id=f'{parent_dag_id}.{subdag_id}',
        default_args=default_args
    ) as dag:
        
        process_a()
        process_b()
        process_c()
        
    return dag