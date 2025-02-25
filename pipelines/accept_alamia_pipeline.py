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

DATE_XPATH = '//div[@title="{}"]/div/span[contains(text(), "{}")]'
OK_BUTTON_XPATH = '//span[contains(text(), "OK")]'
GENERATE_BUTTON_XPATH = '//button[1][contains(text(), "Generate")]'
DOWNLOAD_BUTTON_XPATH = '//table/tbody/tr[1]/td[5]/button[contains(text(), "Download")]'

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

config = credentials[3]
portal_url = config['portal_url']
transactions_url = config['transactions_url']
username = config['aa_username']
password = config['aa_password']

snowflake_conn_paymob = config['snowflake_id']
is_incremental = config['is_incremental']

def check_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")

def paymob_accept_table_info(snowflake_conn_paymob, table_name):
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

        metadata_sql = f"""
            SELECT COUNT(*),
                    MAX(CREATED_AT),
                    MAX(CREATED_AT),
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

def get_last_load_dates(snowflake_conn_id, table):
    """
    Fetches the last load dates for a specific table from the tracking table.
    """

    try:
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        query = f"""
            SELECT TABLE_NAME, MAX_CREATED_AT_DATE 
            FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_NAME = '{table}'
        """
        results = hook.get_first(query)

        table_last_loads = {
                "table_name": results[0],
                "last_load_date": results[1].strftime("%d-%b-%Y") if results[1] else datetime(1970, 1, 1).strftime("%d-%b-%Y"),
        }
        
        return table_last_loads

    except Exception as e:
        logger.error(f"Failed to fetch last load dates: {e}")
        raise



# Extract Step


def wait_for_download_complete(directory, timeout=120):
    """Wait until all .crdownload files disappear"""
    
    # Ensure directory exists
    check_directory(directory)

    end_time = time.time() + timeout
    while time.time() < end_time:
        if not any(fname.endswith('.crdownload') for fname in os.listdir(directory)):
            return True
        time.sleep(1)
    return False

def extract_data(snowflake_conn_paymob, original_table_name):
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
        
        time.sleep(1)

        # Login
        WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((By.ID, "user-name"))
        ).send_keys(username)
        
        driver.find_element(By.ID, "password").send_keys(password)

        WebDriverWait(driver, 50).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@type = "submit"]'))
        ).click()
        
        time.sleep(20)
        
        # Navigate to transactions
        driver.get(transactions_url)

        time.sleep(2)

        if is_incremental:
            last_load_dates = get_last_load_dates(snowflake_conn_paymob, original_table_name)
    

            last_load = last_load_dates.get("last_load_date")
            last_load = last_load.replace('-', ' ')

            day = last_load[:2]
            day = day.lstrip('0')

            if not last_load:
                logger.warning("No valid last load timestamp found")
            
            time.sleep(5)
            # Click on the report type
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//select[@placeholder ="Choose Type..."]'))
            ).click()

            # Choose transactions from the dropdown
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//option[@value ="transactions"]'))
            ).click()

            # Identify the element containing calender from date
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//a[@name ="date"]'))
            ).click()


            print(f'//div[@title="{last_load}"]/div/span[contains(text(), "{day}")]')
            # Find the desired day cell with title "dd Month Year" like "26 Mar 2024"
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, f'//div[@title="{last_load}"]/div/span[contains(text(), "{day}")]'))
            ).click()

            
            # Find the "OK" button
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//span[contains(text(), "OK")]'))
            ).click()

            time.sleep(2)
            
            # Identify the element containing calender To date
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//a[@name ="date range"]'))
            ).click()

            time.sleep(2)

            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//span[contains(text(), "OK")]'))
            ).click()

            # Find the "Generate" button
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((By.XPATH, '//button[1][contains(text(), "Generate")]'))
            ).click()
   
        # wait for the download button to appear.
        time.sleep(60)

        # Reload the transaction reports page.
        driver.get(transactions_url)
        
        # Find the "Generate" button
        WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((By.XPATH, '//table/tbody/tr[1]/td[5]/button[contains(text(), "Download")]'))
        ).click()

        time.sleep(5)

        # Wait for download to complete
        if not wait_for_download_complete(DOWNLOAD_DIR, DOWNLOAD_TIMEOUT):
            raise Exception("Download timed out")

        # Find and rename the downloaded file
        list_of_files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv'))
        
        if not list_of_files:
            available_files = os.listdir(DOWNLOAD_DIR)
            raise Exception(f"No Excel files found. Available files: {available_files}")

        latest_file = max(list_of_files, key=os.path.getctime)
        if is_incremental:
            new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_alamia_incremental_load.csv')
        else:
            new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_alamia_full_load.csv')
        
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




# Transform Step

def transform_data(extracted_data_path):
    
    # Generate unique filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_path = './paymob_data/processed'
    output_path = f"{processed_path}/alamia_transactions_processed.csv"
    os.makedirs(processed_path, exist_ok=True)
    
    new_filename = f"paymob_alamia_{timestamp}.csv"
    dest_path = os.path.join(processed_path, new_filename)
    
    # Rename and process
    os.rename(extracted_data_path, dest_path)
    
    # Data processing
    df = pd.read_csv(dest_path)
    df['paid_at'] = df['paid_at'].replace('None', '')
    df['paid_at'] = df['paid_at'].replace('/', '-', regex=True)
    df['created_at'] = df['created_at'].replace('/', '-', regex=True)
    df['paid_at'] = pd.to_datetime(df['paid_at'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], format='%d-%m-%Y %H:%M:%S')
    
    df.columns = [col.upper() for col in df.columns]
    df.to_csv(output_path, index=False)
    
    return output_path



# Load Step

def handle_new_rows(source_data_path, table_name, snowflake_conn_paymob):
    """
    Identifies new rows between source and destination data.
    Returns:
        - inserts: DataFrame of new rows to be inserted.
        - metadata: Dictionary containing metadata about the incremental load.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)

    destination_data = hook.get_pandas_df(f"SELECT ID FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}")

    source_data = pd.read_csv(source_data_path)

    if not destination_data.empty:
        destination_data["ID"] = destination_data["ID"].astype(str).str.strip()
        existing_ids = set(destination_data["ID"])
    else:
        existing_ids = set()

    source_data["ID"] = source_data["ID"].astype(str).str.strip()

    # Identify new rows (IDs not in destination)
    new_rows = source_data[~source_data["ID"].isin(existing_ids)]

    # Prepare metadata
    metadata = {
        "table_name": table_name,
        "new_rows_count": len(new_rows),
        "updated_rows_count": 0,
        "total_rows_processed": len(source_data),
        "load_timestamp": datetime.now().strftime(DATE_FORMAT),
    }

    logger.info(f"New rows: {metadata['new_rows_count']}")
    return new_rows, metadata

def log_load_metadata(snowflake_conn_paymob, metadata, dag_start_time, dag_finish_time, processing_time_sec, load_type, status="SUCCESS", error_message=None):
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
            new_rows, metadata = checked_data
            rows_inserted = len(new_rows)

            if not new_rows.empty:
                write_pandas(conn, new_rows, table, schema=target_schema)

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
                snowflake_conn_paymob,
                metadata,
                dag_start_time,
                dag_finish_time,
                processing_time_sec,
                load_type,
                status="SUCCESS"
            )

        else:
            df = pd.read_csv(checked_data)

            # Full load
            write_pandas(conn, df, table, schema=target_schema)

        logger.info(f"Data loaded successfully into {table}.")

    except Exception as e:
        if is_incremental:
            # Log metadata with error details
            log_load_metadata(
                snowflake_conn_paymob,
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