import os
import sys
import json
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from filelock import FileLock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.app_pipeline import (
    SPORTS_TABLES,
    DINING_TABLES,
    ACCESS_TABLES,
    NOTIFICATIONS_TABLES,
    WALLETS_PAYMENT_TABLES,
    MEMBERS_TABLES,
    COMMUNITIES_TABLES,
    OTHER_TABLES
)
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
SNOWFLAKE_DB = "KODE_STAGING"
SNOWFLAKE_SCHEMA = "APP"
TRACKING_TABLE = "ETL_CONFIG.TABLE_COUNT"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
META_COUNT_PATH = './data/raw/app/mysql_stats.csv'
SF_META_COUNT_PATH = './data/raw/app/snowflake_stats.csv'

def cleanup_file(mysql_path, sf_path):
    if os.path.exists(mysql_path):
        os.remove(mysql_path)
        logger.info(f"Removed {mysql_path}")
    if os.path.exists(sf_path):
        os.remove(sf_path)
        logger.info(f"Removed {sf_path}")

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

def app_table_info(snowflake_conn_id, table_name, extracted_data):
    """
    Updates metadata for tables in the Snowflake schema.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    has_created_at, has_last_updated = extracted_data[3], extracted_data[4]

    try:
        # Delete old entries from the tracking table
        cursor.execute(f"""
            DELETE FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
            AND TABLE_NAME = '{table_name}'
        """)

        if has_created_at:
            # Get metadata and insert into the tracking table
            metadata_sql = f"""
                SELECT COUNT(*), MAX(CREATED_AT), MAX(UPDATED_AT), MAX(INSERT_DATE), MIN(INSERT_DATE)
                FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}
            """
        else:
            # Get metadata and insert into the tracking table
            metadata_sql = f"""
                SELECT COUNT(*), NULL AS MAX_CREATED_AT, NULL AS MAX_UPDATED_AT, MAX(INSERT_DATE), MIN(INSERT_DATE)
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
        meta_data = {'TABLE_NAME': [table_name], 'SNOWFLAKE_ROW_COUNT': [row_count]}

        mode = 'a' if os.path.exists(SF_META_COUNT_PATH) else 'w'
        header = False if os.path.exists(SF_META_COUNT_PATH) else True
        sf_meta_df = pd.DataFrame(meta_data, columns=['TABLE_NAME', 'SNOWFLAKE_ROW_COUNT'])

        # Write the DataFrame to CSV incrementally
        # Atomic write to CSV with file lock
        lock = FileLock(SF_META_COUNT_PATH + ".lock")
        with lock:
            sf_meta_df.to_csv(
                SF_META_COUNT_PATH,
                mode=mode,
                header=header,
                index=False
            )
        conn.commit()
        logger.info("Metadata updated successfully.")

        # count_query = f"SELECT COUNT(*) FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{target_table.upper()}"
        # source_count = snowflake_hook.get_first(count_query)[0]

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
            WHERE TABLE_NAME = '{table.upper()}'
        """
        results = hook.get_first(query)

        table_last_loads = {
                "table_name": results[0],
                "last_load_date": results[1].strftime(DATE_FORMAT) if results[1] else datetime(1970, 1, 1).strftime(DATE_FORMAT),
        }
        
        return table_last_loads

    except Exception as e:
        logger.error(f"Failed to fetch last load dates: {e}")
        raise
    
# Function to extract data from a specific table in MySQL
def extract_data(mysql_conn_id, snowflake_conn_id, table_name, last_updated_column, is_incremental):
    if is_incremental:
        output_path = f'./data/raw/app/{table_name}_incremental.csv'
    else:
        output_path = f'./data/raw/app/{table_name}.csv'
        
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)

    # Get source row count before extraction
    count_query = f"SELECT COUNT(*) FROM db.{table_name}"
    source_count = hook.get_first(count_query)[0]
    meta_data = {'TABLE_NAME': [table_name], 'SOURCE_ROW_COUNT': [source_count]}

    mode = 'a' if os.path.exists(META_COUNT_PATH) else 'w'
    header = False if os.path.exists(META_COUNT_PATH) else True
    meta_df = pd.DataFrame(meta_data, columns=['TABLE_NAME', 'SOURCE_ROW_COUNT'])

    # Write the DataFrame to CSV incrementally
    lock = FileLock(META_COUNT_PATH + ".lock")
    with lock:
        meta_df.to_csv(
            META_COUNT_PATH,
            mode=mode,
            header=header,
            index=False
        )
    # meta_df.to_csv(
    #     META_COUNT_PATH,
    #     mode=mode,
    #     header=header,
    #     index=False
    # )

    column_query = f"""DESCRIBE db.{table_name};"""
    columns = hook.get_records(column_query)
    field_types = {col[0].lower(): col[1].lower() for col in columns}

    has_last_updated = 'updated_at' in list(field_types.keys())
    has_created_at = 'created_at' in list(field_types.keys())

    
    if is_incremental:
        # Skip table if no timestamp columns found
        if not (has_last_updated or has_created_at):
            logger.warning(f"Skipping incremental load for {table_name} - missing both updated_at and created_at columns")
            return pd.DataFrame()
        
        last_load_dates = get_last_load_dates(snowflake_conn_id, table_name)
        
        # for last_load_data in last_load_dates:
        table_name = last_load_dates.get("table_name").lower()
        last_update = last_load_dates.get("last_load_date")
        query = f"""
            SELECT * FROM {table_name}
            WHERE COALESCE({last_updated_column}, CREATED_AT) >= '{last_update}'
        """
        logger.info(f"Incremental query: {query}")
    else:
        query = f"SELECT * FROM {table_name}"

    df = hook.get_pandas_df(query)
    df.to_csv(output_path, index=False)
    
    return output_path, table_name, field_types, has_created_at, has_last_updated


def is_boolean_column(column_name):
    """Identify boolean columns based on naming patterns."""
    col_lower = column_name.lower()
    boolean_prefixes = {
        'is_', 'has_', 'allow_', 'should_', 'enable', 'disable',
        'needs_', 'did_', 'can_', 'show_', 'hide_', 'use_', 'verified'
    }
    boolean_terms = {
        'active', 'approved', 'enabled', 'hidden', 'seen', 'success',
        'visible', 'status', 'cancelled', 'refunded', 'locked', 'featured',
        'account_created', 'pass_created', 'pinless', 'user_activated', 'admin_activated',
        'need_assessment', 'delivery_option', 'tax_is_percentage'
    }
    
    if any(col_lower.startswith(prefix) for prefix in boolean_prefixes):
        return True
    if any(term in col_lower for term in boolean_terms):
        return True
    return False

def transform_data(extracted_data, is_incremental):
    """
    Transforms the extracted data by formatting dates and converting specific columns to boolean.
    """
    output_exctracted_path, table_name, types = extracted_data[0], extracted_data[1], extracted_data[2]
    if is_incremental:
        output_processsed_path = f'./data/processed/app/{table_name}_incremental.csv'
    else:
        output_processsed_path = f'./data/processed/app/{table_name}.csv'
    
    df = pd.read_csv(output_exctracted_path)
    transformed_data = df.copy()

    # Format columns
    for col in df.columns:
        if types[col] in ('datetime', 'timestamp'):
            transformed_data[col] = pd.to_datetime(transformed_data[col], errors='coerce')
            # transformed_data[col] = transformed_data[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
            # transformed_data[col] = transformed_data[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if not pd.isna(x) else None)
            transformed_data[col] = transformed_data[col].where(
                transformed_data[col].notna(), 
                None  # Explicitly set to None
            )
            transformed_data[col] = transformed_data[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
            
        elif types[col] == "time":
            transformed_data[col] = transformed_data[col].astype(str).str.split(" ").str[-1]

        elif types[col] == "date":
            transformed_data[col] = pd.to_datetime(transformed_data[col], errors='coerce')
            transformed_data[col] = transformed_data[col].dt.strftime('%Y-%m-%d')


        elif types[col] == "tinyint(1)":
            if is_boolean_column(col):
                transformed_data[col] = transformed_data[col].astype(bool)
            else:
                transformed_data[col] = transformed_data[col].fillna(-1).astype(int).replace(-1, None)

        elif types[col] in ('int', 'smallint', 'mediumint', 'bigint', 'tinyint'):
            # transformed_data[col] = pd.to_numeric(transformed_data[col], errors='coerce').fillna(0).astype(int)
            transformed_data[col] = transformed_data[col].fillna(-1).astype(int).replace(-1, None)
            
        elif types[col] in ('decimal', 'numeric', 'float', 'double'):
            transformed_data[col] = transformed_data[col].fillna(-1).astype(float).replace(-1, None)

        elif types[col] == "boolean":
            transformed_data[col] = transformed_data[col].astype(bool)

        else:
            transformed_data[col] = transformed_data[col].fillna('').astype(str)

    transformed_data.columns = [col.upper() for col in transformed_data.columns]   
    transformed_data.to_csv(output_processsed_path, index=False)

    return output_processsed_path, types


def map_dtypes_to_snowflake(column, types):

    src_type = types[column.lower()]

    if src_type in ('datetime', 'timestamp'):
        return 'TIMESTAMP_NTZ'
    
    elif src_type == 'time':
        return 'TIME'
    
    elif src_type == 'date':
        return 'DATE'
    
    elif src_type == 'tinyint(1)':
        return 'BOOLEAN'
    
    elif src_type in ('int', 'smallint', 'mediumint', 'bigint'):
        return 'NUMBER'
    
    elif src_type in ('float', 'double', 'decimal', 'numeric'):
        return 'FLOAT'
    
    elif 'bool' in src_type:
        return 'BOOLEAN'
    
    else:
        return 'STRING'
    
def merge_to_snowflake(df, target_table, key_name, snowflake_conn_id, types):
    """
    Uses a MERGE statement to update existing rows and insert new rows
    into the target Snowflake table from the given DataFrame.
    """
    temp_table = f"{target_table}_STAGING".upper()
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Step 1: Create or replace a temporary staging table
        # We create all columns as STRING here; if needed, adjust the types accordingly.
        create_temp_table_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} (
                {', '.join([f'"{col}" {map_dtypes_to_snowflake(col, types)}' for col in df.columns])}
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
        update_set = ", ".join([f'target."{col}" = staging."{col}"' for col in df.columns if col != key_name])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f'staging."{col}"' for col in df.columns])

        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{target_table.upper()} AS target
            USING {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} AS staging
            ON target."{key_name}" = staging."{key_name}"
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """
        logger.info(f"Executing MERGE statement:\n{merge_sql}")
        cursor.execute(merge_sql)
        
        # Get number of affected rows
        result = cursor.fetchone()
        rows_updated = result[0]  # Number of updated rows
        rows_inserted = result[1]  # Number of inserted rows
        conn.commit()

        logger.info(f"MERGE executed successfully on table {target_table}.")
        logger.info(f"New rows: {rows_inserted}, Updated rows: {rows_updated}")

        return rows_inserted, rows_updated

    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing MERGE: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

def log_load_metadata(snowflake_conn_id, metadata, dag_start_time, dag_finish_time, processing_time_sec, load_type, status="SUCCESS", error_message=None):
    """
    Logs metadata about a load (full or incremental) into a Snowflake table.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
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

def load_data_to_snowflake(snowflake_conn_id, is_incremental, target_table, checked_data, target_schema, **kwargs):
    """
    Loads data into Snowflake, handling both full and incremental loads.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    metadata = {}

    # Capture DAG start time (execution_date)
    dag_start_time = kwargs['execution_date']
    if dag_start_time.tzinfo is not None:
        dag_start_time = dag_start_time.replace(tzinfo=None)  # Strip timezone information
    logger.info(f"DAG start time: {dag_start_time}")

    try:
        if is_incremental:
            # Incremental load
            data_path, types = checked_data[0], checked_data[1]
            df = pd.read_csv(data_path)
            rows_inserted, rows_updated = merge_to_snowflake(df, target_table, "ID", snowflake_conn_id, types)

            # Update metadata for incremental load
            metadata.update({
                "table_name": target_table.upper(),
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "total_rows_processed": (rows_inserted + rows_updated),
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            })
            load_type = "INCREMENTAL"

        else:
            # Full load
            chunksize = 50000  # 50,000 rows per chunk
            rows_inserted = 0

            data_path = checked_data[0]
            # Adding PAYMOB_ORDER_ID because it have mixed types.
            df = pd.read_csv(data_path, chunksize=chunksize,
                             dtype={'PAYMOB_ORDER_ID': str,
                                    'TXN_ID': str,
                                    'REFUND_TRANSACTION_ID': str,})

            # Read the CSV file in chunks
            for chunk in df:
                # Insert the chunk into Snowflake
                write_pandas(conn, chunk, target_table.upper(), schema=target_schema)
                
                # Update the count of rows inserted (this could be logged or tracked)
                rows_inserted += len(chunk)
                print(f"Inserted {rows_inserted} rows so far.")
            # write_pandas(conn, df, target_table.upper(), schema=target_schema)
            # Update metadata for full load
            metadata = {
                "table_name": target_table,
                "rows_inserted": rows_inserted,
                "load_timestamp": datetime.now().strftime(DATE_FORMAT),
            }
            load_type = "FULL"
        
        # Capture finish time and calculate processing time
        dag_finish_time = datetime.now()
        processing_time_sec = int((dag_finish_time - dag_start_time).total_seconds())

        # Log metadata about the load
        log_load_metadata(
            snowflake_conn_id,
            metadata,
            dag_start_time,
            dag_finish_time,
            processing_time_sec,
            load_type,
            status="SUCCESS"
        )
        logger.info(f"Data loaded successfully into {target_table}.")


    except Exception as e:
        if is_incremental:
            # Log metadata with error details
            log_load_metadata(
                snowflake_conn_id,
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
            raise
    finally:
        conn.close()




# def extract_mysql_stats(extracted_data):
#     meta_count_path = extracted_data[5]
#     return meta_count_path

# def extract_snowflake_stats(extracted_data):
#     sf_meta_count_path = extracted_data[0]
#     return sf_meta_count_path


def compare_stats(mysql_data_path,sf_data_path):
    compare_output_path = './data/raw/app/compare.csv'

    df_mysql = pd.read_csv(mysql_data_path)
    df_snowflake = pd.read_csv(sf_data_path)
    
    # Case normalization
    df_mysql['TABLE_NAME'] = df_mysql['TABLE_NAME'].str.upper()
    df_snowflake['TABLE_NAME'] = df_snowflake['TABLE_NAME'].str.upper()
    
    comparison = pd.merge(df_mysql, df_snowflake, on='TABLE_NAME', how='outer')
    
    # Initialize all as mismatch
    comparison['STATUS'] = 'MISMATCH'
    
    # Handle missing tables
    comparison.loc[comparison['SNOWFLAKE_ROW_COUNT'].isna(), 'STATUS'] = 'MISSING_IN_SNOWFLAKE'
    comparison.loc[comparison['SOURCE_ROW_COUNT'].isna(), 'STATUS'] = 'MISSING_IN_SOURCE'
    
    # Calculate difference for tables present in both
    both_present_mask = comparison['STATUS'] == 'MISMATCH'
    comparison['COUNT_DIFF'] = (comparison['SOURCE_ROW_COUNT'] - comparison['SNOWFLAKE_ROW_COUNT']).abs()
    
    # Apply tolerance threshold
    comparison.loc[
        both_present_mask & (comparison['COUNT_DIFF'] < 10),
        'STATUS'
    ] = 'MATCH'
    comparison.to_csv(compare_output_path, index=False)
    # Log results
    logger.info("Validation Results with Tolerance:\n%s", comparison)
    return compare_output_path

def load_comparison_to_snowflake(snowflake_conn_id,compare_stats):
    df_comparison = pd.read_csv(compare_stats)
    
    module_mapping = {
        'SPORTS': SPORTS_TABLES,
        'DINING': DINING_TABLES,
        'ACCESS': ACCESS_TABLES,
        'NOTIFICATIONS': NOTIFICATIONS_TABLES,
        'WALLETS_PAYMENT': WALLETS_PAYMENT_TABLES,
        'MEMBERS': MEMBERS_TABLES,
        'COMMUNITIES': COMMUNITIES_TABLES,
        'OTHER': OTHER_TABLES
    }
    SOURCE_NAME = 'APP'
    # Snowflake connection
    sf_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    
    # Create the target table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS KODE_STAGING.ETL_CONFIG.COMPARISON_RESULTS (
        SOURCE_NAME VARCHAR,
        MODULE_NAME VARCHAR,
        TABLE_NAME VARCHAR,
        SOURCE_ROW_COUNT NUMBER,
        SNOWFLAKE_ROW_COUNT NUMBER,
        STATUS VARCHAR,
        LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    sf_hook.run(create_table_sql)

    # Insert each row into the target table
    for _, row in df_comparison.iterrows():
        # Determine module name based on table name (case-insensitive lookup)
        table_lower = row['TABLE_NAME'].lower()
        module_name = 'UNKNOWN'
        for mod, tables in module_mapping.items():
            if table_lower in [t.lower() for t in tables]:
                module_name = mod
                break

        # Handle possible NULLs appropriately
        source_count = row['SOURCE_ROW_COUNT'] if pd.notna(row['SOURCE_ROW_COUNT']) else 'NULL'
        sf_count = row['SNOWFLAKE_ROW_COUNT'] if pd.notna(row['SNOWFLAKE_ROW_COUNT']) else 'NULL'
        
        insert_sql = f"""
        INSERT INTO KODE_STAGING.ETL_CONFIG.COMPARISON_RESULTS 
        (SOURCE_NAME, MODULE_NAME, TABLE_NAME, SOURCE_ROW_COUNT, SNOWFLAKE_ROW_COUNT, STATUS)
        VALUES ('{SOURCE_NAME}', '{module_name}', '{row['TABLE_NAME']}', {source_count}, {sf_count}, '{row['STATUS']}');
        """
        sf_hook.run(insert_sql)
