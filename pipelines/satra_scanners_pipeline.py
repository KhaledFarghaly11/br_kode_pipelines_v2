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
scanners_url = config['scanners_url']
username = config['username']
password = config['password']
app_token = config['apptoken']

snowflake_conn_satra = config['snowflake_id']


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
            SELECT COUNT(*), MAX(CREATEDDATE), MAX(CREATEDDATE), MAX(INSERT_DATE), MIN(INSERT_DATE)
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
    # Configuration setup
    output_path = f'./data/raw/satra/{table_name}.csv'

    # Use session for connection pooling
    with requests.Session() as session:
        try:
            # Login and get auth token
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
            
            # Perform login
            login_response = session.post(
                login_url, 
                headers=login_headers, 
                data=login_payload
            )
            login_response.raise_for_status()
            token = login_response.json()["data"]["secretkey"]
            
            # Set headers for subsequent requests
            session.headers.update({
                'Authorization': f'Bearer {token}',
                'apptoken': app_token,
                'devicetoken': '123',
                'devicetype': 'iOS'
            })

            response = session.get(scanners_url)
            response.raise_for_status()
            
            batch = response.json()
            if not batch.get('data'):
                logger.info("Data not found!")

            # Write data incrementally to save memory
            df = pd.DataFrame(batch['data'])
            df.to_csv(
                output_path,
                mode='a' if os.path.exists(output_path) else 'w',
                index=False
            )
            
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected response structure: {e}")
            raise

    return output_path


def transform_data(extracted_data_path, table_name):
    """
    Transforms the extracted data by formatting dates and converting specific columns to boolean.
    """
    output_processsed_path = f'./data/processed/satra/{table_name}.csv'
 
    df = pd.read_csv(extracted_data_path)
    transformed_data = df.copy()

    transformed_data.columns = [col.upper() for col in transformed_data.columns]
    transformed_data['CREATEDDATE'] = pd.to_datetime(transformed_data['CREATEDDATE'], errors='coerce')
    transformed_data['CREATEDDATE'] = transformed_data['CREATEDDATE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    
    transformed_data['LASTLOGONDATE'] = pd.to_datetime(transformed_data['LASTLOGONDATE'], errors='coerce')
    transformed_data['LASTLOGONDATE'] = transformed_data['LASTLOGONDATE'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]

    transformed_data.to_csv(output_processsed_path, index=False)

    return output_processsed_path

def load_data_to_snowflake(table, checked_data, target_schema):
    """
    Loads data into Snowflake, handling both full and incremental loads.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_satra)

    conn = hook.get_conn()

    try:
        df = pd.read_csv(checked_data)

        write_pandas(conn, df, table, database=SNOWFLAKE_DB, schema=target_schema, chunk_size=10000)

        logger.info(f"Data loaded successfully into {table}.")

    except Exception as e:
        logger.error(f"Error loading data into Snowflake: {e}")
    finally:
        conn.close()