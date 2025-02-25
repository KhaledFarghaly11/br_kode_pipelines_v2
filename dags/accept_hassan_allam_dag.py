import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.accept_hassan_allam_pipeline import (
    load_json_file,
    paymob_accept_table_info, 
    extract_data,
    transform_data, 
    load_data_to_snowflake, 
    get_last_load_dates
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_NAME = 'PAYMOB'
CREDENTIALS_PATH = './config/credentials.json'
TABLE_NAME = "ACCEPT_HASSANALLAM"

credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[3]
snowflake_conn_paymob = config['snowflake_id']
is_incremental = config['is_incremental']


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12)
}

with DAG(
    'paymob_accept_hassan_allam_transactions_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
) as dag:
    
    # Task to check the last load date for incremental loads
    if is_incremental:
        check_last_load_date_task = PythonOperator(
            task_id=f'check_transactions_last_load_date',
            python_callable=get_last_load_dates,
            op_args=[snowflake_conn_paymob, TABLE_NAME],
            provide_context=True
        )

    extract_data_task = PythonOperator(
        task_id='extract_transactions_data',
        python_callable=extract_data,
        op_args=[snowflake_conn_paymob, TABLE_NAME]

    )

    transform_data_task = PythonOperator(
        task_id='transform_transactions_data',
        python_callable=transform_data,
        op_args=[extract_data_task.output],

    )


    load_data_task = PythonOperator(
        task_id=f'load_transactions_data',
        python_callable=load_data_to_snowflake,
        op_args=[  
            TABLE_NAME,
            transform_data_task.output,
            SCHEMA_NAME
        ],
        provide_context=True
    )

    # Task to update table information
    paymob_table_info_task = PythonOperator(
        task_id=f'paymob_transactions_table_info_task',
        python_callable=paymob_accept_table_info,
        op_args=[snowflake_conn_paymob, TABLE_NAME]
    )

    if is_incremental:
        check_last_load_date_task >> extract_data_task >> transform_data_task  >> load_data_task >> paymob_table_info_task
    else:
        extract_data_task >> transform_data_task >> load_data_task >> paymob_table_info_task