from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

class CustomPostgresOperator(PostgresOperator):
    """To use Jinja templates at parameterized values."""
    template_fields = ('sql', 'parameters')
    

def extract(partner_name):
    partner_setting = Variable.get('my_dag_partner', deserialize_json=True) # {"name": "a", "path": "/path"}
    secret = partner_setting['api_secret']
    path = partner_setting['path']  
    print(f"Partner: {partner_name}, Path: {path}, Secret: {secret}")

with DAG(
    'my_dag',
    description='This is my first DAG',
    start_date=datetime(2021, 1, 1),
    schedule='@weekly',
    dagrun_timeout=timedelta(minutes=10),
    tags=['customers', 'data_engineering'],
    catchup=False,
    max_active_runs=1
) as dag:
    
    # extract_task = PythonOperator(
    #     task_id='extract_task',
    #     python_callable=extract,
    #     op_args=['{{var.json.my_dag_partner.name}}'] # Desse jeito vai chamar conexão ao meta db só quando executar a dag
    #     # op_args=[Variable.get('my_dag_partner', deserialize_json=True)['name']] # Vai chamar conexão ao meta db toda vez que a dag e parseada
    # )
    
    emp_task = EmptyOperator(
        task_id='empty_task'
    )
    
    fetching_data = CustomPostgresOperator(
        task_id='fetching_data',
        postgres_conn_id='postgres_picoles',
        sql='sql/conservantes.sql', # Puxa do arquivo sql
        parameters={
            'next_date': '{{next_ds}}',
            'prev_date': '{{prev_ds}}'
        }
        # sql='SELECT * FROM conservantes WHERE data_criacao = {{ds}};' # 'ds' vem da data da execução da dag, parametro pré pronto do airflow
    )
    
    emp_task >> fetching_data
    
    
    """
    
    first_task = PostgresOperator(
        task_id='create_table',
        sql='CREATE TABLE IF NOT EXISTS my_table (id INTEGER);' # Wrong 'CREATE TABLE my_table (id INTEGER);'
    )
    
    Da erro pois se executar esse comando duas vezes, não deixa criar pasta que ja existe.
    another_task = BashOperator(
        task_id='another_task',
        bash_command='mkdir my_folder'
    )
    
    """
    