import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from itertools import chain

# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.odoo_pipeline import (
    ODOO_META_COUNT_PATH,
    SF_META_COUNT_PATH,

    cleanup_file,
    load_json_file,
    odoo_table_info, 
    extract_data, 
    transform_data, 
    load_data_to_snowflake, 
    get_last_load_dates,
    compare_stats,
    load_comparison_to_snowflake
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
CREDENTIALS_PATH = './config/credentials.json'
MODULES_PATH = './config/modules.json'


modules = load_json_file(MODULES_PATH)
modules_name = modules[0]
include_columns = modules[1]

HR_MODULE = modules_name['hr_module']
POS_MODULE = modules_name['pos_module']
CUSTOMERS_MODULE = modules_name['customers_module']
ACCOUNTING_MODULE = modules_name['accounting_module']
PURCHASE_MODULE = modules_name['purchase_module']
HELPDESK_MODULE = modules_name['helpdesk_module']
SALE_MODULE = modules_name['sale_module']

ALL_MODULES = [
    HR_MODULE,
    POS_MODULE,
    CUSTOMERS_MODULE,
    # ACCOUNTING_MODULE,
    PURCHASE_MODULE,
    HELPDESK_MODULE,
    SALE_MODULE
]
ALL_MODULES = list(chain(*ALL_MODULES))



credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[1]
credential_id = config['credential_id']
snowflake_conn_id = config['snowflake_id']
is_incremental = config['is_incremental']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12)
}

with DAG(
    'odoo_pipeline_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    # schedule="@daily",
    catchup=False
) as dag:
    
    cleanup_group = TaskGroup("cleanup_files_task")
    
    with cleanup_group:
        cleanup_path_task = PythonOperator(
            task_id='clean_path_task',
            python_callable=cleanup_file,
            op_args=[ODOO_META_COUNT_PATH, SF_META_COUNT_PATH,],
            provide_context=True
        )

    with TaskGroup(group_id=credential_id, tooltip=f"Tasks for {credential_id}") as credential_group:
        for table in ALL_MODULES:
            # Task to check the last load date for incremental loads
            if is_incremental:
                check_last_load_date_task = PythonOperator(
                    task_id=f'check_last_load_date_{credential_id}_{table}',
                    python_callable=get_last_load_dates,
                    op_args=[table],
                    provide_context=True
                )

            # Task to extract data
            extract_task = PythonOperator(
                task_id=f'extract_data_{credential_id}_{table}',
                python_callable=extract_data,
                op_args=[table, include_columns.get(table, [])],
                provide_context=True
            )

            # Task to transform data
            transform_task = PythonOperator(
                task_id=f'transform_data_{credential_id}_{table}',
                python_callable=transform_data,
                op_args=[extract_task.output],
                provide_context=True
            )

            # Task to load data into Snowflake
            load_task = PythonOperator(
                task_id=f'load_data_{credential_id}_{table}',
                python_callable=load_data_to_snowflake,
                op_args=[
                    table,
                    transform_task.output
                    # handle_updates_task.output if is_incremental else transform_task.output
                ],
                provide_context=True
            )

            # Task to update table information
            odoo_table_info_task = PythonOperator(
                task_id=f'odoo_table_info_task_{credential_id}_{table}',
                python_callable=odoo_table_info,
                op_args=[table]
            )

            # Define task dependencies
            if is_incremental:
                check_last_load_date_task >> extract_task >> transform_task >> load_task >> odoo_table_info_task
            else:
                extract_task >> transform_task >> load_task >> odoo_table_info_task

    validation_group = TaskGroup("validation_tasks")
    
    with validation_group:

        compare_stats_task = PythonOperator(
            task_id='compare_stats',
            python_callable=compare_stats,
            provide_context=True,
            op_kwargs={
                'odoo_data_path': ODOO_META_COUNT_PATH,
                'sf_data_path': SF_META_COUNT_PATH
            }
        )

        load_comparison_to_snowflake_task = PythonOperator(
            task_id='load_comparison_to_snowflake',
            python_callable=load_comparison_to_snowflake,
            provide_context=True,
            op_kwargs={
                'snowflake_conn_id': snowflake_conn_id,
                'compare_stats': compare_stats_task.output
            }
        )
        
        # mysql_stats >> compare_stats_task
        # snowflake_stats >> compare_stats_task
        compare_stats_task >> load_comparison_to_snowflake_task

    # Set dependency - entire credential group must complete first
    cleanup_group >> credential_group >> validation_group