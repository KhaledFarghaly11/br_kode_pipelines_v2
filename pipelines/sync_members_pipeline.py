import os
import glob
import time
import logging
import json
import pandas as pd

from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants
CREDENTIALS_PATH = './config/credentials.json'
SNOWFLAKE_DB = "KODE_STAGING"
SNOWFLAKE_SCHEMA = "PAYMOB"
TRACKING_TABLE = "ETL_CONFIG.TABLE_COUNT"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

DOWNLOAD_TIMEOUT = 500  # Maximum wait time for download in seconds
SELENIUM_HUB_URL = 'http://remote_chromedriver:4444/wd/hub'
DOWNLOAD_DIR = './paymob_data'  # Match container path

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

config = credentials[2]
portal_url = config['portal_url']
members_url = config['members_url']
username = config['username']
password = config['password']

snowflake_conn_paymob = config['snowflake_id']
is_incremental = config['is_incremental']

def check_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def paymob_sync_table_info(table_name):
    """
    Updates metadata for tables in the Snowflake schema.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

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
            SELECT COUNT(*),
                    MAX(CREATION_DATE),
                    MAX(CREATION_DATE),
                    MAX(INSERT_DATE),
                    MIN(INSERT_DATE)
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
        logger.error(f"Error executing app_table_info: {e}")
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
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

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

def wait_for_download_complete(directory, timeout=60):
    """Wait until all .crdownload files disappear"""
    
    # Ensure directory exists
    check_directory(directory)

    end_time = time.time() + timeout
    while time.time() < end_time:
        if not any(fname.endswith('.crdownload') for fname in os.listdir(directory)):
            return True
        time.sleep(1)
    return False

def extract_data(original_table_name):
    
    try:     
        check_directory(DOWNLOAD_DIR)
        
        # Configure Chrome options
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "download.default_directory": DOWNLOAD_DIR,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Initialize driver
        driver = webdriver.Remote(
            command_executor=SELENIUM_HUB_URL,
            options=chrome_options
        )

        # Your login and navigation code
        driver.get(portal_url)
        
        # Login
        WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((By.ID, "id_Username"))
        ).send_keys(username)
        
        driver.find_element(By.ID, "id_Password").send_keys(password)
        driver.find_element(By.ID, "formbutton").click()
        
        # Navigate to transactions
        driver.get(members_url)
        time.sleep(5)

        if is_incremental:
            last_load_dates = get_last_load_dates(snowflake_conn_paymob, original_table_name)
    
            # for load_data in last_load_dates:
            #     last_load = load_data["last_load_date"]

            # if not last_load:
            #     logger.warning("No valid last load timestamp found")
            #     continue
            last_load = last_load_dates.get("last_load_date")

            if not last_load:
                logger.warning("No valid last load timestamp found")

            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.ID, "start_date"))
            ).send_keys(last_load)

            driver.find_element(By.ID, "formbutton").click()

        
        time.sleep(5)
        # Initiate download
        WebDriverWait(driver, 50).until(
            EC.element_to_be_clickable((By.ID, "export_excel_button"))
        ).click()
        
        time.sleep(30)

        # Wait for download to complete
        if not wait_for_download_complete(DOWNLOAD_DIR, DOWNLOAD_TIMEOUT):
            raise Exception("Download timed out")

        # Find and rename the downloaded file
        list_of_files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.xlsx'))
        
        if not list_of_files:
            available_files = os.listdir(DOWNLOAD_DIR)
            raise Exception(f"No Excel files found. Available files: {available_files}")

        latest_file = max(list_of_files, key=os.path.getctime)
        new_name = os.path.join(DOWNLOAD_DIR, 'paymob_sync_members_full_load.xlsx')
        
        # Remove existing file if it exists
        if os.path.exists(new_name):
            os.remove(new_name)
        
        os.rename(latest_file, new_name)
        print(f"Successfully renamed file to {new_name}")

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise

    finally:
        if 'driver' in locals():
            driver.quit()
    return new_name


def transform_data(extracted_data_path):
    
    # Generate unique filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_path = './paymob_data/processed'
    output_path = f"{processed_path}/members_processed.xlsx"
    os.makedirs(processed_path, exist_ok=True)
    
    new_filename = f"paymob_sync_members_{timestamp}.xlsx"
    dest_path = os.path.join(processed_path, new_filename)
    
    # Rename and process
    os.rename(extracted_data_path, dest_path)
    
    # Data processing
    df = pd.read_excel(dest_path, engine='openpyxl')
    df['Creation Date'] = pd.to_datetime(df['Creation Date'], errors='coerce')
    # %p AM - PM
    df['Creation Date'] = df['Creation Date'].dt.strftime('%Y-%m-%d %H:%M:%S').astype(str)
    df.rename(columns={'First Name': 'first_name', 'Last Name': 'last_name',
                       'Creation Date': 'creation_date'}, inplace=True)
    df.columns = [col.upper() for col in df.columns]
    df.to_excel(output_path, index=False)
    
    return output_path


def update_to_snowflake(df, table_name, key_name):
    """
    Updates existing rows in a Snowflake table using a temporary table.
    """
    temp_table = f"{table_name}_TEMPORARY_TABLE".upper()
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Step 1: Create a temporary table
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} (
                {', '.join([f'"{col}" STRING' for col in df.columns])}
            );
        """)

        # Step 2: Upload DataFrame to the temporary table
        write_pandas(conn, df, temp_table, database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA)

        # Step 3: Construct the UPDATE SQL query
        update_columns = [f'"{col}" = s."{col}"' for col in df.columns if col != key_name]
        update_stmt = f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name} t
            SET {', '.join(update_columns)}
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} s
            WHERE s."{key_name}" = t."{key_name}";
        """

        cursor.execute(update_stmt)
        conn.commit()
        logger.info(f"Update executed successfully on {table_name}")

    except Exception as e:
        logger.error(f"Error executing update query: {e}")
        conn.rollback()
        raise

    finally:
        cursor.close()

def handle_new_rows_and_updates(source_data_path, table_name):
    """
    Identifies new rows between source and destination data.
    Returns:
        - inserts: DataFrame of new rows to be inserted.
        - modified: DataFrame of rows to be updated.
        - metadata: Dictionary containing metadata about the incremental load.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

    destination_data = hook.get_pandas_df(f"SELECT * EXCLUDE INSERT_DATE FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}")

    source_data = pd.read_excel(source_data_path)

    source_data.columns = source_data.columns.str.upper()  # Ensure consistent case
    destination_data.columns = destination_data.columns.str.upper()
    

    # Standardize data types and formatting for ALL columns
    for df in [source_data, destination_data]:
        df.columns = df.columns.str.upper()  # Ensure column names are uppercase
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.strip() 
        df["ID"] = df["ID"].astype(str).str.strip()

    # for df in [source_data, destination_data]:
    #     df["ID"] = df["ID"].astype(str).str.strip()
    
    # Identify new rows (IDs not in destination)
    existing_ids = set(destination_data["ID"]) if not destination_data.empty else set()
    new_rows = source_data[~source_data["ID"].isin(existing_ids)]

    # Identify modified rows (IDs present in both but with different data)
    common_ids = source_data[source_data["ID"].isin(existing_ids)]
    merged = common_ids.merge(
        destination_data, 
        on="ID", 
        suffixes=('_source', '_dest')
    )

    # Find columns to compare (exclude ID and any audit columns)
    compare_cols = [col for col in source_data.columns if col not in ["ID"]]

    # Create modified mask
    modified_mask = False
    for col in compare_cols:
        source_col = f"{col}_source"
        dest_col = f"{col}_dest"
        if source_col in merged.columns and dest_col in merged.columns:
            # Handle NaN/None explicitly
            modified_mask |= (
                merged[source_col].fillna('@@NULL@@') != merged[dest_col].fillna('@@NULL@@')
            )
    
    compare_cols_source = [f"{col}_source" for col in compare_cols]
    modified_rows = merged[modified_mask][['ID'] + compare_cols_source].rename(
        columns=lambda x: x.replace('_source', '') if '_source' in x else x
    )

    # Prepare metadata
    metadata = {
        "table_name": table_name,
        "new_rows_count": len(new_rows),
        "updated_rows_count": len(modified_rows),
        "total_rows_processed": len(source_data),
        "load_timestamp": datetime.now().strftime(DATE_FORMAT),
    }

    logger.info(f"New rows: {metadata['new_rows_count']}, Updated rows: {metadata['updated_rows_count']}")
    return new_rows, modified_rows, metadata

def log_load_metadata(metadata, dag_start_time, dag_finish_time, processing_time_sec, load_type, status="SUCCESS", error_message=None):
    """
    Logs metadata about a load (full or incremental) into a Snowflake table.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

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
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

    conn = hook.get_conn()

    # Capture DAG start time (execution_date)
    dag_start_time = kwargs['execution_date']
    if dag_start_time.tzinfo is not None:
        dag_start_time = dag_start_time.replace(tzinfo=None)  # Strip timezone information
    logger.info(f"DAG start time: {dag_start_time}")

    try:
        if is_incremental:
            new_rows, updated_rows, metadata = checked_data
            rows_inserted = len(new_rows)
            rows_updated = len(updated_rows)

            if not new_rows.empty:
                write_pandas(conn, new_rows, table, database=SNOWFLAKE_DB, schema=target_schema)
            
            if not updated_rows.empty:
                update_to_snowflake(updated_rows, table.upper(), "ID")

            # Update metadata for incremental load
            metadata.update({
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
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
            df = pd.read_excel(checked_data)

            write_pandas(conn, df, table, database=SNOWFLAKE_DB, schema=target_schema)

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