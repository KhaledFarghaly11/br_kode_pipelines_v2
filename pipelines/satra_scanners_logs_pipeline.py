import os
import requests
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

config = credentials[4]
login_url = config['login_url']
logs_url = config['logs_url']
username = config['username']
password = config['password']
app_token = config['apptoken']

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
            SELECT COUNT(*), MAX(ACTIVITYDATE), MAX(ACTIVITYDATE), MAX(INSERT_DATE), MIN(INSERT_DATE)
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

def get_last_load_dates(table):
    """
    Fetches the last load dates for a specific table from the tracking table.
    """

    try:
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

        query = f"""
            SELECT TABLE_NAME, MAX_CREATED_AT_DATE 
            FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_NAME = '{table}'
        """
        results = hook.get_first(query)

        table_last_loads  = {
            "table_name": results[0],
            "last_load_date": results[1].strftime("%Y-%m-%d %H:%M:%S") if results[1] else datetime(1970, 1, 1).strftime("%Y-%m-%d %H:%M:%S"),
        }

        return table_last_loads

    except Exception as e:
        logger.error(f"Failed to fetch last load dates: {e}")
        raise


def extract_data(table_name):
    # Configuration setup
    output_path = f'./data/raw/satra/{table_name}{"_INCREMENTAL" if is_incremental else ""}.csv'

    if os.path.exists(output_path):
        os.remove(output_path)

    try:
        login_payload = json.dumps({
            "mobileno-or-email": username,
            "password": password
        })

        login_headers = {
            'devicetoken': '123',
            'apptoken': app_token,
            'deviceType': 'Android',
            'Content-Type': 'application/json'
        }

        
        login_response = requests.request("POST", login_url, headers=login_headers, data=login_payload)

        # print(response.text)
        login = json.loads(login_response.text)

        token = login["data"]["secretkey"]

        logs_headers = {
            'Authorization': f'Bearer Bearer {token}',
            'apptoken': app_token,
            'devicetoken': '123',
            'devicetype': 'iOS'
        }

        if is_incremental:
            last_load_dates = get_last_load_dates(table_name)
            last_load_date = last_load_dates.get("last_load_date")
            logger.info(f"New Date is: {last_load_date}.")
        else:
            last_load_date = None

        data_list =[]
        for page in range(0, 10000):
            params = {
                'start_date': last_load_date,
                'size': '50',
                'page': page
            }
            response = requests.request("GET", logs_url, headers=logs_headers, data={}, params=params)
            batch = json.loads(response.text)
            
            # Check if there is no data and exit loop if so
            if not batch.get('data'):
                logger.info(f"No data found on page {page}. Exiting loop.")
                break
            
            # Convert the data to a DataFrame
            df = pd.DataFrame(batch['data'])
            data_list.append(df)
        
            mode = 'a' if page > 0 else 'w'
            # Write header only for the first page
            header = (page == 0)
            
            # Write the DataFrame to CSV incrementally
            df.to_csv(
                output_path,
                mode=mode,
                header=header,
                index=False
            )
                
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise


    return output_path


def transform_data(extracted_data_path, table_name):
    """
    Transforms the extracted data by formatting dates and converting specific columns to boolean.
    """
    output_processsed_path = f'./data/processed/satra/{table_name}{"_INCREMENTAL" if is_incremental else ""}.csv'
 
    df = pd.read_csv(extracted_data_path)
    transformed_data = df.copy()

    transformed_data.columns = [col.upper() for col in transformed_data.columns]
    transformed_data['ACTIVITYDATE'] = pd.to_datetime(transformed_data['ACTIVITYDATE'], errors='coerce')
    transformed_data['ACTIVITYDATE'] = transformed_data['ACTIVITYDATE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]

    transformed_data.columns = [col.upper() for col in transformed_data.columns]
    transformed_data.to_csv(output_processsed_path, index=False)

    return output_processsed_path


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
            # new_rows, metadata = checked_data
            # rows_inserted = len(new_rows)

            # if not new_rows.empty:
            new_rows = pd.read_csv(checked_data, dtype={'CLIENTMOBILE': str})

            write_pandas(conn, new_rows, table, database=SNOWFLAKE_DB, schema=target_schema, chunk_size=10000)

            metadata = {
                "table_name": table,
                "new_rows_count": len(new_rows),
                "updated_rows_count": 0,
                "total_rows_processed": len(new_rows),
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            }

            # Update metadata for incremental load
            metadata.update({
                "rows_inserted": len(new_rows),
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
            df = pd.read_csv(checked_data, dtype={'CLIENTMOBILE': str})

            # Full load
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