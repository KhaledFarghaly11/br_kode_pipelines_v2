import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.satra_scanners_pipeline import (
    load_json_file,
    extract_data,
    transform_data, 
    load_data_to_snowflake,
    satra_table_info 
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_NAME = 'SATRA'
CREDENTIALS_PATH = './config/credentials.json'
TABLE_NAME = "SCANNERS"

credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[4]
snowflake_conn_satra = config['snowflake_id']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12)
}

with DAG(
    'satra_scanners_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_scanners_data',
        python_callable=extract_data,
        op_args=[TABLE_NAME]
    )

    transform_data_task = PythonOperator(
        task_id='transform_scanners_data',
        python_callable=transform_data,
        op_args=[extract_data_task.output, TABLE_NAME]
    )

    load_data_task = PythonOperator(
        task_id=f'load_scanners_data',
        python_callable=load_data_to_snowflake,
        op_args=[  
            TABLE_NAME,
            transform_data_task.output,
            SCHEMA_NAME
        ],
        provide_context=True
    )

    # Task to update table information
    satra_table_info_task = PythonOperator(
        task_id=f'satra_scanners_table_info_task',
        python_callable=satra_table_info,
        op_args=[TABLE_NAME]
    )


    extract_data_task >> transform_data_task >> load_data_task >> satra_table_info_task