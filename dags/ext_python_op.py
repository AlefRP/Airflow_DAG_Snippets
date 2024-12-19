from airflow import DAG
from airflow.operators.python import ExternalPythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

# Define a função que irá importar e executar o script
def run_create_tables(postgres_conn):
    import sys
    import json
    
    # If postgres_conn is a string, ensure it is properly formatted as JSON (replace single quotes)
    if isinstance(postgres_conn, str):
        # Replace single quotes with double quotes for JSON validity
        postgres_conn = postgres_conn.replace("'", '"')
    
    # Now try to parse it into a dictionary
    try:
        postgres_conn = json.loads(postgres_conn)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    
    scripts_path = '/usr/local/airflow/dags/scripts'
    
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
    
    __import__('create_tables').main(host=postgres_conn['host'],
                                     port=postgres_conn['port'],
                                     user=postgres_conn['user'],
                                     password=postgres_conn['password'],
                                     db_name=postgres_conn['database'])
    
    if scripts_path in sys.path:
        sys.path.remove(scripts_path)

with DAG(
    dag_id='ext_python_op',
    description='DAG using ExternalPythonOperator with virtual environment',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'analytics'],
    catchup=False,
) as dag:

    create_tables_task = ExternalPythonOperator(
        task_id='create_tables_task',
        python='/home/astro/sqla/bin/python',  # Caminho do Python no ambiente virtual
        python_callable=run_create_tables,  # Passa a função definida
        task_concurrency=1,
        op_args=['{{var.json.postgres_conn}}']
    )
