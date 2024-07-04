from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


SOURCE_DATABASE = 'RAW'
SOURCE_SCHEMA   = 'NBA_DUMP'
TARGET_DATABASE = 'CLEAN'
TARGET_SCHEMA   = 'NBA'
ENTITIES        = ['games']

default_args    = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1
}

def create_table(database, schema, table, columns, connection):
    '''
    create a new table in snowflake
    '''
    col_len = len(columns)-1
    query = f"create or replace transient table {database}.{schema}.{table}("
    for idx, col in enumerate(columns):
        query += f"{col} varchar"
        if idx != col_len:
            query += ", "
    query += ");"

    connection.execute(query)

def update_schema():
    '''
    get columns from staging table and update prod table if exists
    '''
    snowf_engine = SnowflakeHook(
        'snowflake_conn'
        , database = SOURCE_DATABASE
        , schema = SOURCE_SCHEMA
    ).get_sqlalchemy_engine()

    with snowf_engine.begin() as con:
        source_query    = f"select column_name from {SOURCE_DATABASE}.information_schema.columns where table_name = 'GAMES' and table_schema = '{SOURCE_SCHEMA}';"
        source_res      = con.execute(source_query).fetchall()
        
        if not source_res:
            return
        
        source_columns  = [col[0] for col in source_res]
        
        target_query    = f"select column_name from {TARGET_DATABASE}.information_schema.columns where table_name = 'GAMES' and table_schema = '{TARGET_SCHEMA}';"
        target_res      = con.execute(target_query).fetchall()
        if not target_res:
            create_table(
                database=TARGET_DATABASE
                , schema=TARGET_SCHEMA
                , table = 'GAMES'
                , columns = source_columns
                , connection = con
            )
        else:
            target_columns  = [col[0] for col in target_res]
            new_cols        = [col for col in source_columns if col not in set(target_columns)]

            
            if not new_cols or len(new_cols) == 0:
                return

            add_col_query   = f"alter table {TARGET_DATABASE}.{TARGET_SCHEMA}.GAMES add column "   
            new_cols_num    = len(new_cols)-1
            for idx,col in enumerate(new_cols):
                add_col_query += f"{col} varchar"
                if idx != new_cols_num:
                    add_col_query += ', '
            add_col_query += ';'

            con.execute(add_col_query)

with DAG(dag_id='nba_games', default_args=default_args, schedule_interval=None) as dag:

    # Extract and Load to staging area
    # extract_load = BashOperator(

    #     task_id         = 'extract_load'
    #     , bash_command  = f'docker exec nba-sport-extractor-app-1 python main.py '\
    #                         f' --entity games'\
    #                         f' --database {SOURCE_DATABASE}'\
    #                         f' --schema {SOURCE_SCHEMA}'
    # )

    # Update table schema
    update_schema = PythonOperator(
        task_id = 'update_schema'
        , python_callable = update_schema
    )

    # Merge into prod table

    # Clean up 
    
    # extract_load >> update_schema
    update_schema