import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta


# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.sync_transactions_pipeline import (
    load_json_file,
    paymob_sync_table_info, 
    extract_data,
    transform_data, 
    load_data_to_snowflake, 
    get_last_load_dates
    # handle_new_rows
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_NAME = 'PAYMOB'
CREDENTIALS_PATH = './config/credentials.json'
TABLE_NAME = "SYNC_TRANSACTIONS"

credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[2]
snowflake_conn_paymob = config['snowflake_id']
is_incremental = config['is_incremental']


def track_task(context):
    """Logs task execution details to Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)
    conn = hook.get_conn()
    cursor = conn.cursor()

    ti = context['task_instance']
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context['execution_date'].isoformat()
    start_time = ti.start_date.isoformat() if ti.start_date else None
    end_time = ti.end_date.isoformat() if ti.end_date else None
    duration = (ti.end_date - ti.start_date).total_seconds() if ti.end_date and ti.start_date else None
    status = ti.state
    error_message = str(ti.exception).replace("'", "''") if status == 'failed' else None  # Escape single quotes

    # Create the target table if it doesn't exist
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS KODE_STAGING.ETL_CONFIG.TASK_TRACKING  (
            ID VARCHAR DEFAULT UUID_STRING(),
            SOURCE_NAME VARCHAR,
            TASK_ID VARCHAR,
            DAG_ID VARCHAR,
            EXECUTION_DATE TIMESTAMP,
            START_TIME TIMESTAMP,
            END_TIME TIMESTAMP,
            DURATION NUMBER,
            STATUS VARCHAR,
            ERROR_MESSAGE VARCHAR,
            LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );
    """
    hook.run(create_table_sql)

    try:
        insert_sql = f"""
            INSERT INTO KODE_STAGING.ETL_CONFIG.TASK_TRACKING
            (SOURCE_NAME, TASK_ID, DAG_ID, EXECUTION_DATE, START_TIME, END_TIME, DURATION, STATUS, ERROR_MESSAGE)
            VALUES (
                'PAYMOB-SYNC',
                '{task_id}',
                '{dag_id}',
                '{execution_date}',
                {'NULL' if not start_time else f"'{start_time}'"},
                {'NULL' if not end_time else f"'{end_time}'"},
                {duration or 'NULL'},
                '{status}',
                {'NULL' if error_message is None else f"'{error_message}'"}
            );
        """
        cursor.execute(insert_sql)
        conn.commit()
        logger.info(f"Logged task {task_id} status: {status}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error logging task {task_id}: {str(e)}")
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'on_success_callback': track_task,
    'on_failure_callback': track_task
}

with DAG(
    'paymob_sync_transactions_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
) as dag:
    
    # Task to check the last load date for incremental loads
    if is_incremental:
        check_last_load_date_task = PythonOperator(
            task_id='check_transactions_last_load_date',
            python_callable=get_last_load_dates,
            op_args=[TABLE_NAME],
            provide_context=True
        )

    extract_data_task = PythonOperator(
        task_id='extract_transactions_data',
        python_callable=extract_data,
        op_args=[TABLE_NAME]

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
        python_callable=paymob_sync_table_info,
        op_args=[TABLE_NAME]
    )

    if is_incremental:
        # check_last_load_date_task >> 
        extract_data_task >> transform_data_task  >> load_data_task >> paymob_table_info_task
    else:
        extract_data_task >> transform_data_task >> load_data_task >> paymob_table_info_task