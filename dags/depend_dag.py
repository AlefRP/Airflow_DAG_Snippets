from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import cross_downstream, chain
from datetime import datetime


default_args = {
    'start_date': datetime(2023, 1, 1)
}
with DAG(
    dag_id='depend_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    t1 = EmptyOperator(task_id='t1')
    t2 = EmptyOperator(task_id='t2')
    t3 = EmptyOperator(task_id='t3')
    t4 = EmptyOperator(task_id='t4')
    t5 = EmptyOperator(task_id='t5')
    t6 = EmptyOperator(task_id='t6')
    
    t7 = EmptyOperator(task_id='t7')
    
    # How to set dependencies between tasks
    # t2.set_upstream(t1) # t2 << t1
    # t1.set_downstream(t2) # t1 >> t2
    
    # t1 >> t2
    # t2 << t1
    
    # Fazer dependencias de duas listas
    # cross_downstream([t1, t2, t3], [t4, t5, t6]) # se fizer >> t7 não funciona, tenho que fazer conforme abaixo
    # [t4, t5, t6] >> t7
    
    # chain(t1, [t2, t3], [t4, t5], t6, t7)
    
    cross_downstream([t2, t3], [t4, t5])
    chain(t1, t2, t5, t6, t7)