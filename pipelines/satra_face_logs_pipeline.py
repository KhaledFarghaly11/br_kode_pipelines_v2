import glob
import os
import logging
import json
import pandas as pd

from datetime import datetime, timedelta

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants
CREDENTIALS_PATH = './config/credentials.json'
SNOWFLAKE_DB = "KODE_STAGING"
SNOWFLAKE_SCHEMA = "SATRA"
TRACKING_TABLE = "ETL_CONFIG.TABLE_COUNT"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DATA_URL = './data/raw/satra/face_logs'
LIMIT_LAST_ROWS = 20

def load_json_file(file_path):
    """Load configurations from a JSON file."""
    try:
        with open(file_path, 'r') as file:
            json_file = json.load(file)
        
        if not json_file:
            raise ValueError("No data found in the file.")
        
        logger.info("file loaded successfully.")
        return json_file
    except Exception as e:
        logger.error(f"Error loading file: {e}")
        raise

credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[5]

snowflake_conn_satra = config['snowflake_id']
is_incremental = config['is_incremental']


def check_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")


def satra_table_info(table_name):
    """
    Updates metadata for tables in the Snowflake schema.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Delete old entries from the tracking table
        cursor.execute(f"""
            DELETE FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
            AND TABLE_NAME = '{table_name}'
        """)

        # Get metadata and insert into the tracking table
        metadata_sql = f"""
            SELECT COUNT(*), MAX(EVENT_TIME), MAX(EVENT_TIME), MAX(INSERT_DATE), MIN(INSERT_DATE)
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}
        """
        cursor.execute(metadata_sql)
        metadata = cursor.fetchone()

        row_count, max_created_at, max_updated_at, max_insert_date, min_insert_date = metadata

        # Step 4: Insert metadata into the tracking table
        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DB}.{TRACKING_TABLE}
            (TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, MAX_CREATED_AT_DATE, MAX_UPDATED_AT_DATE, MAX_INSERT_DATE, MIN_INSERT_DATE, LOAD_TIMESTAMP)
            VALUES
            ('{SNOWFLAKE_SCHEMA}', '{table_name}', {row_count}, 
            {'NULL' if max_created_at is None else f"'{max_created_at.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if max_updated_at is None else f"'{max_updated_at.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if max_insert_date is None else f"'{max_insert_date.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if min_insert_date is None else f"'{min_insert_date.strftime(DATE_FORMAT)}'"}, 
            '{(datetime.now()+timedelta(hours=2)).strftime(DATE_FORMAT)}')
        """
        cursor.execute(insert_sql)

        conn.commit()
        logger.info("Metadata updated successfully.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing table_info: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

    return "Done!"


def extract_data(table_name):
    if is_incremental:
        # Find and rename the downloaded file
        list_of_files = glob.glob(os.path.join(DATA_URL, '*.xlsx'))
        
        if not list_of_files:
            available_files = os.listdir(DATA_URL)
            raise Exception(f"No Excel files found. Available files: {available_files}")

        latest_file = max(list_of_files, key=os.path.getctime)
        output_path = os.path.join(DATA_URL, f'face_logs_incremental.xlsx')

        # Remove existing file if it exists
        if os.path.exists(output_path):
            os.replace(output_path, output_path)
        
        os.rename(latest_file, output_path)
        print(f"Successfully renamed file to {output_path}")
        return output_path
    else:
        output_path = f'./data/raw/satra/{table_name}.xlsx'
        return output_path


def transform_data(extracted_data_path, table_name):
    """
    Transforms the extracted data by formatting dates and converting specific columns to boolean.
    """
    output_processsed_path = f'./data/processed/satra/{table_name}{"_INCREMENTAL" if is_incremental else ""}.xlsx'
 
    df = pd.read_excel(extracted_data_path, engine='openpyxl')
    transformed_data = df.copy()

    transformed_data.columns = [col.upper() for col in transformed_data.columns]
    transformed_data['EVENT_TIME'] = pd.to_datetime(transformed_data['EVENT_TIME'], errors='coerce')
    transformed_data['EVENT_TIME'] = transformed_data['EVENT_TIME'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]

    transformed_data.to_excel(output_processsed_path, index=False)

    return output_processsed_path

def handle_new_rows(source_data_path, table_name):
    """
    Identifies new rows between source and destination data.
    Returns:
        - inserts: DataFrame of new rows to be inserted.
        - metadata: Dictionary containing metadata about the incremental load.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

    # Update the query to select the composite key columns.
    query = f"""
        SELECT USER_ID, UNIQUE_ID, EVENT_TIME, AUTH_RESULT
        FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}
        ORDER BY EVENT_TIME DESC
        LIMIT {LIMIT_LAST_ROWS}
    """
    destination_data = hook.get_pandas_df(query)

    source_data = pd.read_excel(source_data_path, engine='openpyxl')

    # Define the key columns for the composite primary key.
    key_columns = ['USER_ID', 'UNIQUE_ID', 'EVENT_TIME', 'AUTH_RESULT']

    # Ensure that the key columns are in string format and whitespace is removed.
    for col in key_columns:
        if col in destination_data.columns:
            destination_data[col] = destination_data[col].astype(str).str.strip()
        source_data[col] = source_data[col].astype(str).str.strip()

    # Create a composite key in both dataframes.
    # This joins the values from the key columns with an underscore.
    destination_data["composite_key"] = destination_data[key_columns].agg('_'.join, axis=1)
    source_data["composite_key"] = source_data[key_columns].agg('_'.join, axis=1)

    # Get the set of existing composite keys from the destination.
    existing_keys = set(destination_data["composite_key"]) if not destination_data.empty else set()

    # Identify new rows: rows in source_data with composite_key not in existing_keys.
    new_rows = source_data[~source_data["composite_key"].isin(existing_keys)]

    new_rows.drop(columns=['composite_key'], inplace=True)

    # Prepare metadata
    metadata = {
        "table_name": table_name,
        "new_rows_count": len(new_rows),
        "updated_rows_count": 0,
        "total_rows_processed": len(source_data),
        "load_timestamp": datetime.now().strftime(DATE_FORMAT),
    }

    logger.info(f"New rows: {metadata['new_rows_count']}, Updated rows: 0")
    return new_rows, metadata

def log_load_metadata(metadata,dag_start_time, dag_finish_time, processing_time_sec, load_type, status="SUCCESS", error_message=None):
    """
    Logs metadata about a load (full or incremental) into a Snowflake table.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Create the metadata logging table if it doesn't exist
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{TRACKING_TABLE}_LOADS (
                LOAD_TYPE STRING,
                TABLE_NAME STRING,
                NEW_ROWS_COUNT INTEGER,
                UPDATED_ROWS_COUNT INTEGER,
                TOTAL_ROWS_PROCESSED INTEGER,
                LOAD_TIMESTAMP TIMESTAMP,
                DAG_START_TIME TIMESTAMP,
                DAG_FINISH_TIME TIMESTAMP,
                PROCESSING_TIME_SEC INTEGER,
                STATUS STRING,
                ERROR_MESSAGE STRING
            );
        """)

        # Insert metadata into the table
        insert_query = f"""
            INSERT INTO {SNOWFLAKE_DB}.{TRACKING_TABLE}_LOADS
            (LOAD_TYPE, TABLE_NAME, NEW_ROWS_COUNT, UPDATED_ROWS_COUNT, TOTAL_ROWS_PROCESSED, LOAD_TIMESTAMP, DAG_START_TIME, DAG_FINISH_TIME, PROCESSING_TIME_SEC, STATUS, ERROR_MESSAGE)
            VALUES (
                '{load_type}',
                '{metadata["table_name"]}',
                {metadata.get("new_rows_count", 0)},
                {metadata.get("updated_rows_count", 0)},
                {metadata.get("total_rows_processed", 0)},
                '{metadata["load_timestamp"]}',
                '{dag_start_time}',
                '{dag_finish_time}',
                {processing_time_sec},
                '{status}',
                {'NULL' if error_message is None else f"'{error_message}'"}
            );
        """
        cursor.execute(insert_query)
        conn.commit()
        logger.info(f"Logged {load_type} load metadata for table {metadata['table_name']}.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error logging load metadata: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def load_data_to_snowflake(table, checked_data, target_schema, **kwargs):
    """
    Loads data into Snowflake, handling both full and incremental loads.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

    conn = hook.get_conn()

    # Capture DAG start time (execution_date)
    dag_start_time = kwargs['execution_date']
    if dag_start_time.tzinfo is not None:
        dag_start_time = dag_start_time.replace(tzinfo=None)  # Strip timezone information
    logger.info(f"DAG start time: {dag_start_time}")

    try:
        if is_incremental:
            new_rows, metadata = checked_data
            rows_inserted = len(new_rows)

            if not new_rows.empty:
            # new_rows = pd.read_excel(checked_data, engine='openpyxl')
                write_pandas(conn, new_rows, table, database=SNOWFLAKE_DB, schema=target_schema, chunk_size=10000)

            metadata = {
                "table_name": table,
                "new_rows_count": rows_inserted,
                "updated_rows_count": 0,
                "total_rows_processed": rows_inserted,
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            }

            # Update metadata for incremental load
            metadata.update({
                "rows_inserted": rows_inserted,
                "rows_updated": 0,
            })
            load_type = "INCREMENTAL"

            # Capture finish time and calculate processing time
            dag_finish_time = datetime.now()
            processing_time_sec = int((dag_finish_time - dag_start_time).total_seconds())

            # Log metadata about the load
            log_load_metadata(
                metadata,
                dag_start_time,
                dag_finish_time,
                processing_time_sec,
                load_type,
                status="SUCCESS"
            )

        else:
            df = pd.read_excel(checked_data, engine='openpyxl')

            write_pandas(conn, df, table, database=SNOWFLAKE_DB, schema=target_schema, chunk_size=10000)

        logger.info(f"Data loaded successfully into {table}.")

    except Exception as e:
        if is_incremental:
            # Log metadata with error details
            log_load_metadata(
                metadata,
                dag_start_time,
                datetime.now(),
                int((datetime.now() - dag_start_time).total_seconds()),
                "INCREMENTAL",
                status="FAILED",
                error_message=str(e)
            )
            logger.error(f"Error loading data into Snowflake: {e}")
            raise
        else:
            logger.error(f"Error loading data into Snowflake: {e}")
    finally:
        conn.close()