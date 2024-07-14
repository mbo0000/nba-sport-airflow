# nba-sport-airflow
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green)
![Snowflake](https://img.shields.io/badge/Snowflake-%23f3f1ff)
![Docker](https://img.shields.io/badge/Docker-%2B-blue)

## Introduction
This repository contains a data pipeline designed to process NBA Sport API data. The workflow is capable of extracting, loading data, updating target Snowflake table schemas, merging new and updated records, and optionally triggering downstream workflows.

## Table of Contents

- [Introduction](#introduction)
- [Objective](#objective)
- [Architecture](#architecture)
- [Workflow](#workflow)
- [Installation and Setup](#installation-and-setup)
- [Code Walkthrough](#code-walkthrough)
- [Future Work and Improvement](#future-work-and-improvement)
  
## Objective 
The primary objective of this project is to automate the data pipeline with scalable solution.

## Architecture
- **Snowflake**: our data warehouse
- **Python**: data extraction and upload engine
- **Airflow**: schedule and orchestrate workflow and tasks
- **Docker** : project container. For ease of setup, Snowflake connection and Airflow are installed and run in container.

![Diagram](https://github.com/mbo0000/nba-sport-airflow/blob/main/img/arch_diag.png)


## Workflow
In this project, we will be working with multiple endpoints that need to be extracted, loaded, and processed. Each endpoint will have a dedicated DAG generated with specific configurations. Each DAG will include the following tasks:

- **Extract and Load**: Extracts NBA data using a Dockerized Python scripts via bash command. For scripts detail, please visit [this](https://github.com/mbo0000/nba-sport-extractor) repo. 
- **Update Target Table Schema**: Checks and updates the target schema if new columns are added to the source schema.
- **Data Merge**: Merges new and updated records from the source to the target tables in Snowflake.
- **Cleanup**: Drops the source table after processing is completed.
- **Trigger Downstream DAGs**: Optionally triggers additional DAG that has cross DAG dependency.
  
![Diagram](https://github.com/mbo0000/nba-sport-airflow/blob/main/img/dag_tasks_flow.png)

## Installation and Setup

### Prerequisites

- Python 3.9+
- Snowflake Account
- Apache Airflow (2.x recommended)
- Docker
- [API Sport Account](https://api-sports.io)

Below are the steps to locally setup the project:
1. Create a main project folder >> navigate to the project folder.
2. Clone the API extractor repository and follow [installation](https://github.com/mbo0000/nba-sport-extractor) instruction:
3. Clone the airflow repository in the main project directory:
    ```sh
    git clone https://github.com/mbo0000/nba-sport-airflow.git
    cd nba-sport-airflow
4. To enable both containers communicate locally, edit Airflow repo Docker compose file: x-airflow-common >> under volume, paste:
    ```
    - /var/run/docker.sock:/var/run/docker.sock
5. Run container and install dependencies:
    ```sh
    docker build -t airflow-nba-image:latest . && docker compose up -d
6. Once Airflow web UI is up and running, go to [http:localhost:8080](http:localhost:8080) to see the Airflow UI. The username and password are both `airflow`.
7. Create a Snowflake connection and provide your Snowflake credentials
    ![Diagram](https://github.com/mbo0000/nba-sport-airflow/blob/main/img/snowf_conn.png)


## Code Walkthrough
Each DAG is generated using the DAG generator, with configurations defined below. Depending on the configuration, the internal behaviors of each DAG will differ. Currently, there are only 2 endpoints/entities from which to extract data.
```
ENTITIES = [
    {
        'games': {
            'id'                : 'id'
            , 'case_field'      : 'status_long'
            , 'trigger_dag_id'  : 'games_statistics'
            , 'schedule'        : '@daily'
        }
    }
    , {
        'games_statistics': {
            'id'                : 'game_id'
            , 'case_field'      : ''
            , 'trigger_dag_id'  : None
            , 'schedule'        : None
        }
    }
]

for ent in ENTITIES:
    for k,val in ent.items():
        dag_id              = f"nba_{k}"
        globals()[dag_id]   = generate_dag(
            dag_id
            , k
            , val['case_field']
            , val['id']
            , val['trigger_dag_id']
            , val['schedule']
        )

# view DAG for code detail
def generate_dag(dag_id, entity, case_field, entity_id, trigger_dag_id, schedule):
    ...

```
Generated DAG will have the following template: 
1. Extract and load entity data to Snowflake via bash command with arguments:
    ```
    extract_load = BashOperator(
        task_id         = 'extract_load'
        , bash_command  = f'docker exec nba-sport-extractor-app-1 python main.py '\
                            f' --entity {entity}'\
                            f' --database {SOURCE_DATABASE}'\
                            f' --schema {SOURCE_SCHEMA}'
        )
    ```
2. Update target table schema if there are changes in the source table. If target table does not exist, create the target table.
    ```
    update_table_schema = PythonOperator(
        task_id             = 'update_table_schema'
        , python_callable   = func_update_table_schema
        , op_kwargs         = {'table' : entity.upper()}
    )

    # view DAG for code detail
    def func_update_table_schema(ti, table, **kwargs):
        ...
    ```
3. Load source data into target table when there are new records and update to existing records if their values changed since the last run:
    ```
    merge_tables = PythonOperator(
        task_id             = 'merge_tables'
        , python_callable   = func_merge_tables
        , op_kwargs         = {
                                'entity'        : entity
                                , 'table'       : entity.upper()
                                , 'case_field'  : case_field
                                , 'entity_id'   : entity_id
                            }
    )

    # view DAG for code detail
    def func_merge_tables(ti, entity, table, case_field, entity_id, **kwargs):
        ...
    ```
4. Clean up by removing the source table. We do not have further use for it.
    ```
    clean_up =  PythonOperator(
        task_id             = 'clean_up'
        , python_callable   = func_job_clean_up
        , op_kwargs         = {'table' : entity.upper()}
    )

    # view DAG for code detail
    def func_job_clean_up(table):
        ...
    ```
5. Each DAG will trigger another entity's DAG, if applicable. The upstream DAG (games entity) is configured to trigger games_statistics DAG as the last step. This is specified in the trigger_dag_id parameter of the entity. That said, games_statistics DAG does not have a schedule run and only can be trigger manually or by games DAG. 
    ```
    check_dag_downstream = BranchPythonOperator(
        task_id = 'check_dag_downstream'
        , python_callable = func_check_down_stream_dag
        , op_kwargs = {'down_stream_dag_id' : trigger_dag_id}
    )

    trigger_downstream_dag = TriggerDagRunOperator(
        task_id = 'trigger_downstream_dag'
        , trigger_dag_id = f"nba_{trigger_dag_id}"
    )

    # view DAG for code detail
    def func_check_down_stream_dag(down_stream_dag_id):
        ...
    ```

## Future Work and Improvement
- Add other endpoints
- Add data transform, validation and processing to the target tables. Ensuring data is ready for consumption.
- Add logs for ease of debugging for future works.
- Add tests to data pipeline.
- Integrate BI solution and create reports.

## Limitations
The NBA Sport account for this project is a free tier account with a daily usage limit of 100 and a maximum of 10 requests per minute. To adhere to these rate limits, some tables may not have the latest data immediately.

This project is free to use. Thank you checking it out!
