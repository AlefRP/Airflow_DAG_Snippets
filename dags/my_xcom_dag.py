from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def _extract(ti):
    partner_name = 'netflix'
    # ti.xcom_push(key='partner_name', value=partner_name)
    partner_path = '/path'
    # return partner_name # return partner_name # same as using ti.xcom_push
    return {'partner_name': partner_name, 'partner_path': partner_path} 
    
def _transform(ti):
    # partner_name = ti.xcom_pull(task_ids='extract', key='partner_name')
    partner_settings = ti.xcom_pull(task_ids='extract')
    # print(partner_name)
    print(partner_settings['partner_name'])

with DAG(
    'my_xcom_dag',
    description='This is my XCOM DAG',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=_extract
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=_transform
    )
    
    extract >> transform