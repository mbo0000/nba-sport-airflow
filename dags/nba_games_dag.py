from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
}


def test_conn():
    snowf_engine = SnowflakeHook(
        'snowflake_conn'
        , database = 'CLEAN'
        , schema = 'NBA'
    ).get_sqlalchemy_engine()

    with snowf_engine.begin() as con:
        con.execute('create or replace transient table GAMES (ID VARCHAR);')

with DAG(dag_id='nba_games', default_args=default_args, schedule_interval=None) as dag:

    # extract_load = BashOperator(

    #     task_id = 'extract_load'
    #     , bash_command='docker exec nba-sport-extractor-app-1 python main.py --entity games'
    # )

    
    test_con = PythonOperator(
        task_id = 'test_con'
        , python_callable = test_conn
    )
    test_con
    
    # extract_load