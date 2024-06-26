from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
}

with DAG(dag_id='demo_dag', default_args=default_args, schedule_interval=None) as dag:

    run_py = BashOperator(

        task_id = 'run_py'
        , bash_command='docker exec nba-sport-extractor-app-1 python main.py test'
    )

    run_py