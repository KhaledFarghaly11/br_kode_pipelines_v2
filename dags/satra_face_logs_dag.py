import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.satra_face_logs_pipeline import (
    load_json_file,
    satra_table_info, 
    extract_data,
    transform_data, 
    load_data_to_snowflake,
    handle_new_rows
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_NAME = 'SATRA'
CREDENTIALS_PATH = './config/credentials.json'
TABLE_NAME = "FACIAL_LOGS"

credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[5]
snowflake_conn_satra = config['snowflake_id']
is_incremental = config['is_incremental']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12)
}

with DAG(
    'satra_face_logs_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_logs_data',
        python_callable=extract_data,
        op_args=[TABLE_NAME]
    )

    transform_data_task = PythonOperator(
        task_id='transform_logs_data',
        python_callable=transform_data,
        op_args=[extract_data_task.output, TABLE_NAME]
    )

    # Task to handle updates for incremental loads
    if is_incremental:
        handle_new_rows_task = PythonOperator(
            task_id=f'handle_logs_new_rows',
            python_callable=handle_new_rows,
            op_args=[transform_data_task.output, TABLE_NAME, ],
            provide_context=True
        )

    load_data_task = PythonOperator(
        task_id=f'load_logs_data',
        python_callable=load_data_to_snowflake,
        op_args=[  
            TABLE_NAME,
            handle_new_rows_task.output if is_incremental else transform_data_task.output,
            SCHEMA_NAME
        ],
        provide_context=True
    )

    # Task to update table information
    satra_table_info_task = PythonOperator(
        task_id=f'satra_logs_table_info_task',
        python_callable=satra_table_info,
        op_args=[TABLE_NAME]
    )

    # if is_incremental:
    #     # check_last_load_date_task >> 
    #     extract_data_task >> transform_data_task >> load_data_task >> satra_table_info_task
    # else:
    extract_data_task >> transform_data_task >> load_data_task >> satra_table_info_task