# Seu arquivo DAG no Airflow

import logging
import os
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from create_schema_and_models import create_schema_and_models
from create_table import create_table
from data_profile import analyse_dataset

AIRFLOW_LOGGER = logging.getLogger("airflow.task")
base_path = "datasets"
initial_project_file_path = os.path.join("scripts", "config", "schema.json")
create_table_config_file_path = os.path.join("scripts", "config", "column_type_config.json")
data_config_file_path = os.path.join("scripts", "config", "bronze", "column_types.json")
datasets_file_path = os.path.join(base_path)

with DAG(
    dag_id='data_initial_setup',
    start_date=datetime(2025, 1, 1),
    schedule=None, # Rodar sob demanda, ou defina um cron
    catchup=False
) as dag:
    
    # 1. Criação de Schemas e Modelos
    task_create_schemas = PythonOperator(
        task_id='create_schemas',
        python_callable=create_schema_and_models,
        op_kwargs={
        'file_path_config': initial_project_file_path,
        'logger': AIRFLOW_LOGGER 
        }
    )

    # 2. Análise e Geração de Configuração
    task_analyse_data = PythonOperator(
        task_id='analyse_dataset',
        python_callable=analyse_dataset,
        op_kwargs={
        'base_path': base_path,
        'file_path_config': create_table_config_file_path,
        'output_path': data_config_file_path,
        'logger': AIRFLOW_LOGGER 
        }
    )

    # # 3. Criação da Tabela DDL/DML
    # task_create_db_table = PythonOperator(
    #     task_id='create_db_table',
    #     python_callable=create_table,
    #     # op_kwargs (argumentos da função)
    # )

    # Define a ordem de execução (dependências)
    task_create_schemas >> task_analyse_data 