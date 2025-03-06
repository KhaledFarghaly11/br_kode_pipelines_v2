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

config = credentials[3]
portal_url = config['portal_url']
transactions_url = config['transactions_url']
username = config['ha_username']
password = config['ha_password']

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

# def extract_data(snowflake_conn_paymob, original_table_name):
    
#     try:     
#         check_directory(DOWNLOAD_DIR)
        
#         # Configure Chrome options
#         chrome_options = webdriver.ChromeOptions()
#         prefs = {
#             "download.default_directory": DOWNLOAD_DIR,
#             "download.prompt_for_download": False,
#             "download.directory_upgrade": True,
#             "safebrowsing.enabled": True
#         }
#         chrome_options.add_experimental_option("prefs", prefs)
        
#         # Initialize driver
#         driver = webdriver.Remote(
#             command_executor=SELENIUM_HUB_URL,
#             options=chrome_options
#         )

#         # Your login and navigation code
#         driver.get(portal_url)
        
#         time.sleep(1)

#         # Login
#         WebDriverWait(driver, 50).until(
#             EC.presence_of_element_located((By.ID, "user-name"))
#         ).send_keys(username)
        
#         driver.find_element(By.ID, "password").send_keys(password)

#         WebDriverWait(driver, 50).until(
#                 EC.element_to_be_clickable((By.XPATH, '//button[@type = "submit"]'))
#         ).click()
        
#         time.sleep(20)
        
#         # Navigate to transactions
#         driver.get(transactions_url)

#         time.sleep(2)

#         if is_incremental:
#             last_load_dates = get_last_load_dates(snowflake_conn_paymob, original_table_name)
    
            
#             last_load = last_load_dates.get("last_load_date")
#             last_load = last_load.replace('-', ' ')

#             day = last_load[:2]
#             day = day.lstrip('0')

#             if not last_load:
#                 logger.warning("No valid last load timestamp found")
            
#             time.sleep(5)
#             # Click on the report type
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//select[@placeholder ="Choose Type..."]'))
#             ).click()

#             # Choose transactions from the dropdown
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//option[@value ="transactions"]'))
#             ).click()

#             # Identify the element containing calender from date
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//a[@name ="date"]'))
#             ).click()


#             # Find the desired day cell with title "dd Month Year" like "26 Mar 2024"
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, f'//div[@title="{last_load}"]/div/span[contains(text(), "{day}")]'))
#             ).click()

            
#             # Find the "OK" button
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//span[contains(text(), "OK")]'))
#             ).click()

#             time.sleep(2)
            
#             # Identify the element containing calender To date
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//a[@name ="date range"]'))
#             ).click()

#             time.sleep(2)

#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//span[contains(text(), "OK")]'))
#             ).click()

#             # Find the "Generate" button
#             WebDriverWait(driver, 50).until(
#                 EC.presence_of_element_located((By.XPATH, '//button[1][contains(text(), "Generate")]'))
#             ).click()
   
#         # wait for the download button to appear.
#         time.sleep(50)

#         # Reload the transaction reports page.
#         driver.get(transactions_url)
        
#         time.sleep(50)
#         # Find the "Generate" button
#         WebDriverWait(driver, 50).until(
#             EC.presence_of_element_located((By.XPATH, '//table/tbody/tr[1]/td[5]/button[contains(text(), "Download")]'))
#         ).click()

#         time.sleep(5)

#         # Wait for download to complete
#         if not wait_for_download_complete(DOWNLOAD_DIR, DOWNLOAD_TIMEOUT):
#             raise Exception("Download timed out")

#         # Find and rename the downloaded file
#         list_of_files = glob.glob(os.path.join(DOWNLOAD_DIR, '*.csv'))
        
#         if not list_of_files:
#             available_files = os.listdir(DOWNLOAD_DIR)
#             raise Exception(f"No Excel files found. Available files: {available_files}")

#         latest_file = max(list_of_files, key=os.path.getctime)
#         if is_incremental:
#             new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_hassan_allam_incremental_load.csv')
#         else:
#             new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_hassan_allam_full_load.csv')
        
#         # Remove existing file if it exists
#         if os.path.exists(new_name):
#             os.remove(new_name)
        
#         os.rename(latest_file, new_name)
#         print(f"Successfully renamed file to {new_name}")

#     except Exception as e:
#         print(f"Error occurred: {str(e)}")
#         raise

#     finally:
#         if 'driver' in locals():
#             driver.quit()
#     return new_name

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

        uname = driver.find_element(By.ID, "user-name") 
        uname.send_keys(username)
        
        # driver.find_element(By.ID, "password").send_keys(password)
        pword = driver.find_element(By.ID, "password") 
        pword.send_keys(password)

        time.sleep(2)

        submit_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                                                    (By.XPATH, '//button[@type = "submit"]')))
        submit_button.click()

        time.sleep(10)
        
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

            report_type_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                                            (By.XPATH, '//select[@placeholder ="Choose Type..."]')))
            report_type_button.click()


            report_type_button_option = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                                            (By.XPATH, '//option[@value ="transactions"]')))
            report_type_button_option.click()

            
            calender_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//a[@name ="date"]')))
            calender_container.click()


            # Find the desired day cell with title "dd Month Year" like "26 Mar 2024"
            day_container = WebDriverWait(driver, 20).until(EC.presence_of_element_located(
                (By.XPATH, f'//div[@title="{last_load}"]/div/span[contains(text(), "{day}")]')))
            day_container.click()

            # Find the "OK" button
            ok_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//span[contains(text(), "OK")]')))
            ok_container.click()

            time.sleep(2)
            
            calender_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//a[@name ="date range"]')))
            calender_container.click()

            time.sleep(2)

            ok_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//span[contains(text(), "OK")]')))
            ok_container.click()

            generate_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
                (By.XPATH, '//button[1][contains(text(), "Generate")]')))
            generate_container.click()
   
        # wait for the download button to appear.
        time.sleep(220)

        # Reload the transaction reports page.
        driver.get(transactions_url)
        time.sleep(10)
        

        download_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located(
            (By.XPATH, '//table/tbody/tr[1]/td[5]/button[contains(text(), "Download")]')))
        # download_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//table/tbody/tr[1]/td[5]/button')))
        download_container.click()

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
            new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_hassan_allam_incremental_load.csv')
        else:
            new_name = os.path.join(DOWNLOAD_DIR, 'paymob_accept_hassan_allam_full_load.csv')
        
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
    output_path = f"{processed_path}/hassan_allam_transactions_processed.csv"
    os.makedirs(processed_path, exist_ok=True)
    
    new_filename = f"paymob_hassan_allam_{timestamp}.csv"
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


def merge_to_snowflake(df, table_name):
    """
    Uses a MERGE statement to update existing rows and insert new rows
    into the target Snowflake table from the given DataFrame.
    """
    temp_table = f"{table_name}_STAGING".upper()
    key_name = "ID"
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Step 1: Create or replace a temporary staging table
        # We create all columns as STRING here; if needed, adjust the types accordingly.
                # {', '.join([f'"{col}" STRING' for col in df.columns])}
        create_temp_table_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} (
                ID NUMBER,
                AMOUNT_WHOLE FLOAT,
                PAYMENT_METHOD STRING,
                CURRENCY STRING,
                TRX_SOURCE STRING,
                CARD_TYPE STRING,
                SUCCESS BOOLEAN,
                PENDING BOOLEAN,
                CREATED_AT TIMESTAMP_NTZ,
                PAID_AT TIMESTAMP_NTZ,
                IS_LIVE BOOLEAN,
                INTEGRATION STRING,
                ROUTING_BANK STRING,
                CARD_HOLDER_BANK STRING,
                ORDER_TITLE STRING,
                CLIENT_NAME STRING,
                CLIENT_EMAIL STRING,
                CLIENT_PHONE STRING,
                PRODUCT_NAME STRING,
                DESCRIPTION STRING,
                EXTRA_DESCRIPTION STRING,
                API_SOURCE STRING,
                MERCHANT_ORDER_ID STRING,
                IS_3D_SECURE BOOLEAN,
                IS_AUTH BOOLEAN,
                IS_CAPTURE BOOLEAN,
                IS_STANDALONE_PAYMENT BOOLEAN,
                IS_VOID BOOLEAN,
                IS_REFUND BOOLEAN,
                IS_VOIDED BOOLEAN,
                IS_REFUNDED BOOLEAN,
                IS_CAPTURED BOOLEAN,
                PARENT_TRANSACTION STRING,
                ERROR_OCCURED BOOLEAN,
                ERROR_DATA STRING,
                REFUNDED_AMOUNT FLOAT,
                CAPTURED_AMOUNT FLOAT,
                TRX_AMOUNT_CENTS FLOAT,
                CONVERTED_GROSS_AMOUNT STRING,
                TRX_SETTLEMENT_CURR STRING,
                FEES STRING,
                CONVERSION_FX STRING,
                VAT STRING,
                TERMINAL_ID STRING,
                OTHER_ENDPOINT_REFERENCE STRING,
                IS_BANK_INSTALLMENT BOOLEAN,
                INSTALLMENT_BANK_NAME STRING,
                BANK_INSTALLMENT_TENURE STRING,
                TERMINAL_BRANCH_ID STRING,
                TERMINAL_USERNAME STRING,
                DELIVERY_FEES STRING,
                PRE_CONVERSION_CURRENCY STRING,
                PRE_CONVERSION_AMOUNT_CENTS STRING,
                PURCHASE_REF STRING,
                LOAN_ID STRING,
                CREATED_BY STRING,
                MERCHANT_STAFF_TAG STRING,
                CARD_AUTH_CODE STRING,
                IN_TRANSFER BOOLEAN,
                TRANSFER_ID STRING,
                BATCH STRING,
                CLIENT_CITY STRING,
                CLIENT_COUNTRY STRING,
                RECEIPT_NO STRING,
                BEFORE_DISCOUNT_AMOUNT_CENTS STRING,
                AFTER_DISCOUNT_AMOUNT_CENTS STRING,
                DISCOUNT_AMOUNT_CENTS STRING,
                TRANSACTION_SOURCE STRING,
                DATA_MESSAGE_EXECL STRING,
                ADMINISTRATIVE_FEES STRING,
                DOWN_PAYMENT STRING,
                TENURE STRING,
                FINANCED_AMOUNT STRING,
                SPLIT_DESCRIPTION STRING
            );
        """

        cursor.execute(create_temp_table_sql)
        logger.info(f"Temporary staging table {temp_table} created.")

        # Step 2: Upload the DataFrame to the temporary staging table
        write_pandas(conn, df, temp_table, database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA)
        logger.info(f"Data uploaded to temporary staging table {temp_table}.")

        # Step 3: Construct the MERGE SQL statement
        # Build column lists for both update and insert operations
        columns = [f'"{col}"' for col in df.columns]
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f'staging."{col}"' for col in df.columns])

        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name.upper()} AS target
            USING {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} AS staging
            ON target."{key_name}" = staging."{key_name}"
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """
        logger.info(f"Executing MERGE statement:\n{merge_sql}")
        cursor.execute(merge_sql)
        
        # Get number of affected rows
        result = cursor.fetchone()
        rows_inserted = result[0]  # Number of inserted rows
        conn.commit()

        logger.info(f"MERGE executed successfully on table {table_name}.")
        logger.info(f"New rows: {rows_inserted}")

        return rows_inserted

    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing MERGE: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

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

def load_data_to_snowflake(table, data_path, target_schema, **kwargs):
    """
    Loads data into Snowflake, handling both full and incremental loads.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_paymob)
    conn = hook.get_conn()
    metadata = {}

    # Capture DAG start time (execution_date)
    dag_start_time = kwargs['execution_date']
    if dag_start_time.tzinfo is not None:
        dag_start_time = dag_start_time.replace(tzinfo=None)  # Strip timezone information
    logger.info(f"DAG start time: {dag_start_time}")

    try:
        if is_incremental:
            df = pd.read_csv(data_path)
            rows_inserted = merge_to_snowflake(df, table)


            # Update metadata for incremental load
            metadata.update({
                "table_name": table.upper(),
                "rows_inserted": rows_inserted,
                "rows_updated": 0,
                "total_rows_processed": rows_inserted,
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            })
            load_type = "INCREMENTAL"

        else:
            chunksize = 50000  # 50,000 rows per chunk
            rows_inserted = 0

            df = pd.read_csv(data_path, chunksize=chunksize)
            
            # Read the CSV file in chunks
            for chunk in df:
                # Insert the chunk into Snowflake
                write_pandas(conn, chunk, table.upper(), database=SNOWFLAKE_DB, schema=target_schema)
                
                # Update the count of rows inserted (this could be logged or tracked)
                rows_inserted += len(chunk)
                print(f"Inserted {rows_inserted} rows so far.")
            # write_pandas(conn, df, table, database=SNOWFLAKE_DB, schema=target_schema)
            metadata = {
                "table_name": table.upper(),
                "rows_inserted": rows_inserted,
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            }
            load_type = "FULL"

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