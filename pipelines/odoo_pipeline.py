import os
import json
import time
import pandas as pd
import logging
from typing import Tuple
import xmlrpc.client
from http.client import IncompleteRead
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from filelock import FileLock


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants
CREDENTIALS_PATH = './config/credentials.json'
MODULES_PATH = './config/modules.json'
SNOWFLAKE_DB = "KODE_STAGING"
SNOWFLAKE_SCHEMA = "ODOO"
TRACKING_TABLE = "ETL_CONFIG.TABLE_COUNT"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LIMIT = 1000
ODOO_META_COUNT_PATH = './data/raw/odoo/odoo_stats.csv'
SF_META_COUNT_PATH = './data/raw/odoo/snowflake_stats.csv'

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

config = credentials[1]
credential_id = config['credential_id']
url = config['url']
db = config['db']
username = config['username']
password = config['password']
snowflake_conn_id = config['snowflake_id']
is_incremental = config['is_incremental']

modules = load_json_file(MODULES_PATH)
modules_name = modules[0]

HR_MODULE = modules_name['hr_module']
POS_MODULE = modules_name['pos_module']
CUSTOMERS_MODULE = modules_name['customers_module']
ACCOUNTING_MODULE = modules_name['accounting_module']
PURCHASE_MODULE = modules_name['purchase_module']
HELPDESK_MODULE = modules_name['helpdesk_module']
SALE_MODULE = modules_name['sale_module']

#connection paramterers 
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))

models.execute_kw(db, uid, password, 'ir.model.fields', 'check_access_rights', ['read'], {'raise_exception': False})

def cleanup_file(mysql_path, sf_path):
    if os.path.exists(mysql_path):
        os.remove(mysql_path)
        logger.info(f"Removed {mysql_path}")
    if os.path.exists(sf_path):
        os.remove(sf_path)
        logger.info(f"Removed {sf_path}")

def odoo_table_info(table):
    """
    Updates metadata for tables in the Snowflake schema.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    if table == "res.partner":
        table_name = "contacts"
    elif table == "account.move":
        table_name = "journal.entry"
    elif table == "account.move.line":
        table_name = "journal.item"
    elif table == "sr.pdc.payment":
        table_name = "pdc.payment"
    else:
        table_name = table

    table = table_name.replace('.', '_')
    table = table.upper()

    try:

        # Delete old entries from the tracking table
        cursor.execute(f"""
            DELETE FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
            AND TABLE_NAME = '{table}'
        """)

        # For each table, get metadata and insert into the tracking table
        metadata_sql = f"""
            SELECT COUNT(*), MAX(CREATE_DATE), MAX(__LAST_UPDATE), MAX(INSERT_DATE), MIN(INSERT_DATE)
            FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table}
        """
        cursor.execute(metadata_sql)
        metadata = cursor.fetchone()

        row_count, max_created_at, max_updated_at, max_insert_date, min_insert_date = metadata

        # Step 4: Insert metadata into the tracking table
        insert_sql = f"""
            INSERT INTO {SNOWFLAKE_DB}.{TRACKING_TABLE}
            (TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, MAX_CREATED_AT_DATE, MAX_UPDATED_AT_DATE, MAX_INSERT_DATE, MIN_INSERT_DATE, LOAD_TIMESTAMP)
            VALUES
            ('{SNOWFLAKE_SCHEMA}', '{table}', {row_count}, 
            {'NULL' if max_created_at is None else f"'{max_created_at.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if max_updated_at is None else f"'{max_updated_at.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if max_insert_date is None else f"'{max_insert_date.strftime(DATE_FORMAT)}'"}, 
            {'NULL' if min_insert_date is None else f"'{min_insert_date.strftime(DATE_FORMAT)}'"}, 
            '{(datetime.now()+timedelta(hours=2)).strftime(DATE_FORMAT)}')
        """
        cursor.execute(insert_sql)

        meta_data = {'TABLE_NAME': [table], 'SNOWFLAKE_ROW_COUNT': [row_count]}

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
    if table == "res.partner":
        table_name = "contacts"
    elif table == "account.move":
        table_name = "journal.entry"
    elif table == "account.move.line":
        table_name = "journal.item"
    elif table == "sr.pdc.payment":
        table_name = "pdc.payment"
    else:
        table_name = table

    table = table_name.replace('.', '_')
    table = table.upper()

    try:
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        query = f"""
            SELECT TABLE_NAME, MAX_UPDATED_AT_DATE 
            FROM {SNOWFLAKE_DB}.{TRACKING_TABLE}
            WHERE TABLE_NAME = '{table}'
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

    

# def extract_data(table_name, include_columns):
#     target_table = 'hr.employee' if table_name == 'hr.employee.archived' else table_name

#     if is_incremental:
#         last_load_dates = get_last_load_dates(snowflake_conn_id, table_name)
#         # for last_load_data in last_load_dates:
#         table_name = last_load_dates.get("table_name").lower()
#         last_update = last_load_dates.get("last_load_date")
#         if last_update:
#             condition = ['write_date','>',last_update]

#             output_exctracted_incremental_path = f'./data/raw/odoo/{target_table}_incremental.csv'

#             count = models.execute_kw(db, uid, password, target_table,
#                                 'search_count', [[condition]] if condition else [[]])
            
#             logger.info(f"Incremental load count: {count}")
            
#             fields = models.execute_kw(db, uid, password, target_table,
#                                 'fields_get', [], {'attributes': ['string', 'help', 'type']})
            
#             fields_subset = [field for field in include_columns if field in fields]
#             field_types = {field: fields[field]['type'] for field in fields_subset}
            
#             # Initialize an empty DataFrame with the desired columns
#             # df = pd.DataFrame(columns=fields_subset)
            
#             # Generate offsets for batch processing
#             offsets = range(0, count + 1, LIMIT)

#             with open(output_exctracted_path, 'w') as f:
#                 # Write header
#                 # df.to_csv(f, index=False)
#                 pd.DataFrame(columns=fields_subset).to_csv(f, index=False)

#                 for offset in offsets:
#                     logger.info(f"Starting batch: {offset}")
#                     try:
#                         data = models.execute_kw(db, uid, password, target_table, 'search_read', 
#                                                 [[condition]] if condition else [[]],
#                                                 {'fields': fields_subset, 'limit': LIMIT, 'offset': offset})
#                         # data_list.extend(data)

#                         # Create DataFrame from the collected data
#                         df_batch  = pd.DataFrame(data, columns=fields_subset)

#                         # Replace False values with empty strings
#                         df_batch.replace(to_replace=False, value='', inplace=True)
                        
#                         # Append the batch to the CSV file
#                         df_batch.to_csv(f, mode='a', header=False, index=False)

#                         # Sleep to avoid overloading the API (adjust as needed)
#                         time.sleep(1)
#                     except IncompleteRead:
#                         logger.warning(f"IncompleteRead error at offset {offset}. Retrying...")
#                         continue  
#         else:
#             logger.warning("No write_date value found!")

#         logger.info(f"Total record count: {count}, Module name: {target_table}")
#         return output_exctracted_incremental_path, target_table, field_types 
#     else:
#         condition = ['active', '=', False] if table_name == 'hr.employee.archived' else None

#         output_exctracted_path = f'./data/raw/{target_table}.csv'

#         count = models.execute_kw(db, uid, password, target_table,
#                                   'search_count', [[condition]] if condition else [[]])

#         fields = models.execute_kw(db, uid, password, target_table,
#                                    'fields_get', [], {'attributes': ['string', 'help', 'type']})
        
        
#         fields_subset = [field for field in include_columns if field in fields]
#         field_types = {field: fields[field]['type'] for field in fields_subset}
        
        
#         # Generate offsets for batch processing
#         offsets = range(0, count + 1, LIMIT)
        
#         with open(output_exctracted_path, 'w') as f:
#             # Write header
#             # df.to_csv(f, index=False)
#             pd.DataFrame(columns=fields_subset).to_csv(f, index=False)

#             for offset in offsets:
#                 logger.info(f"Starting batch: {offset}")
#                 try:
#                     data = models.execute_kw(db, uid, password, target_table, 'search_read', 
#                                             [[condition]] if condition else [[]],
#                                             {'fields': fields_subset, 'limit': LIMIT, 'offset': offset})
#                     # data_list.extend(data)

#                     # Create DataFrame from the collected data
#                     df_batch  = pd.DataFrame(data, columns=fields_subset)

#                     # Replace False values with empty strings
#                     df_batch.replace(to_replace=False, value='', inplace=True)
                    
#                     # Append the batch to the CSV file
#                     df_batch.to_csv(f, mode='a', header=False, index=False)

#                     # Sleep to avoid overloading the API (adjust as needed)
#                     time.sleep(1)
#                 except IncompleteRead:
#                     logger.warning(f"IncompleteRead error at offset {offset}. Retrying...")
#                     continue       
        

#         logger.info(f"Total record count: {count}, Module name: {target_table}")
#         return output_exctracted_path, target_table, field_types  
    
#     return 'Exctracted data successfully'



def extract_data(table_name: str, 
                 include_columns: list,
                 **kwargs) -> Tuple[str, str, dict]:
    """Extract data from specified table either incrementally or fully."""
    logger = kwargs.get('logger', logging.getLogger(__name__))
    target_table = _get_target_table(table_name)

    if is_incremental:
        return _handle_incremental_load(target_table, table_name, 
                                      include_columns, logger)
    return _handle_full_load(target_table, table_name, include_columns, logger)

def _get_target_table(original_table_name: str) -> str:
    """Determine target table name based on naming conventions."""
    return 'hr.employee' if original_table_name == 'hr.employee.archived' else original_table_name

def _handle_incremental_load(target_table: str,
                           original_table_name: str,
                           include_columns: list,
                           logger) -> Tuple[str, str, dict]:
    """Handle incremental data load with last update tracking."""
    last_load_dates = get_last_load_dates(original_table_name)
    
    # for load_data in last_load_dates:
    # table_name = load_data["table_name"].lower()
    last_update = last_load_dates.get("last_load_date")
    
    if not last_update:
        logger.warning("No valid last update timestamp found")
        logger.warning("No valid incremental load data found")
        # return './data/raw/odoo/default.csv', target_table, {}

    condition = ['write_date', '>', last_update]
    output_path = f'./data/raw/odoo/{original_table_name}_incremental.csv'
    
    field_types, count = _process_data(
        target_table=target_table,
        condition=condition,
        include_columns=include_columns,
        output_path=output_path,
        original_table_name=original_table_name,
        logger=logger
    )
    
    logger.info(f"Incremental load complete. Records: {count}, Table: {target_table}")
    return output_path, target_table, field_types, original_table_name

def _handle_full_load(target_table: str,
                    original_table_name: str,
                    include_columns: list,
                    logger) -> Tuple[str, str, dict]:
    """Handle full table data load."""
    condition = ['active', '=', False] if original_table_name == 'hr.employee.archived' else None
    output_path = f'./data/raw/odoo/{original_table_name}.csv'
    
    field_types, count = _process_data(
        target_table=target_table,
        condition=condition,
        include_columns=include_columns,
        output_path=output_path,
        original_table_name=original_table_name,
        logger=logger
    )
    
    logger.info(f"Full load complete. Records: {count}, Table: {target_table}")
    return output_path, target_table, field_types, original_table_name

def _process_data(target_table: str,
                condition: list,
                include_columns: list,
                output_path: str,
                original_table_name: str,
                logger) -> Tuple[dict, int]:
    """Common data processing logic for both incremental and full loads."""
    try:
        # Get record count
        count = models.execute_kw(
            db, uid, password, target_table,
            'search_count', 
            [[condition]] if condition else [[]]
        )

        if original_table_name == "res.partner":
            table_name = "contacts"
        elif original_table_name == "account.move":
            table_name = "journal.entry"
        elif original_table_name == "account.move.line":
            table_name = "journal.item"
        elif original_table_name == "sr.pdc.payment":
            table_name = "pdc.payment"
        else:
            table_name = original_table_name

        table = table_name.replace('.', '_')
        table = table.upper()

        meta_data = {'TABLE_NAME': [table], 'SOURCE_ROW_COUNT': [count]}

        mode = 'a' if os.path.exists(ODOO_META_COUNT_PATH) else 'w'
        header = False if os.path.exists(ODOO_META_COUNT_PATH) else True
        meta_df = pd.DataFrame(meta_data, columns=['TABLE_NAME', 'SOURCE_ROW_COUNT'])

        # Write the DataFrame to CSV incrementally
        lock = FileLock(ODOO_META_COUNT_PATH + ".lock")
        with lock:
            meta_df.to_csv(
                ODOO_META_COUNT_PATH,
                mode=mode,
                header=header,
                index=False
            )
    except xmlrpc.client.Fault as e:
        logger.error(f"XML-RPC Fault encountered while processing table {target_table}: {str(e)}")
        if 'Invalid field' in str(e):
            logger.error(f"Odoo API error for {target_table} due to invalid field in query condition.")
        # Re-raise to mark task as failed and skip downstream tasks
        raise AirflowException(f"Odoo API error for {target_table}") from e
    
    # Get field metadata
    fields = models.execute_kw(
        db, uid, password, target_table,
        'fields_get', 
        [], 
        {'attributes': ['string', 'help', 'type']}
    )
    
    # Filter and validate columns
    valid_columns = [col for col in include_columns if col in fields]
    field_types = {col: fields[col]['type'] for col in valid_columns}
    
    # Batch processing
    _write_data_to_csv(
        target_table=target_table,
        condition=condition,
        columns=valid_columns,
        output_path=output_path,
        total_count=count,
        logger=logger
    )
    
    return field_types, count

def _write_data_to_csv(target_table: str,
                     condition: list,
                     columns: list,
                     output_path: str,
                     total_count: int,
                     logger):
    """Batch process records and write to CSV."""
    with open(output_path, 'w') as f:
        pd.DataFrame(columns=columns).to_csv(f, index=False)
        
        for offset in range(0, total_count + 1, LIMIT):
            logger.info(f"Processing batch: {offset}/{total_count}")
            try:
                data = models.execute_kw(
                    db, uid, password, target_table,
                    'search_read',
                    [[condition]] if condition else [[]],
                    {'fields': columns, 'limit': LIMIT, 'offset': offset}
                )
                
                df_batch = pd.DataFrame(data, columns=columns)
                df_batch.replace(False, '', inplace=True)
                df_batch.to_csv(f, mode='a', header=False, index=False)
                
                time.sleep(0.5)  # Rate limiting
                
            except IncompleteRead:
                logger.warning(f"Retrying batch at offset {offset}")
                continue

    logger.info(f"CSV generation complete: {output_path}")



def transform_data(extracted_data):
    """
    Transforms the extracted data by formatting dates and converting specific columns to boolean.
    """
    output_exctracted_path, table_name, types, original_table_name = extracted_data[0], extracted_data[1], extracted_data[2], extracted_data[3]
    if is_incremental:
        output_processsed_path = f'./data/processed/odoo/{original_table_name}_incremental.csv'
    else:
        output_processsed_path = f'./data/processed/odoo/{original_table_name}.csv'

    df = pd.read_csv(output_exctracted_path)
    transformed_data = df.copy()

    # Format datetime columns
    for col in df.columns:
        if types[col] == "datetime":
            transformed_data[col] = pd.to_datetime(transformed_data[col], errors='coerce')
            transformed_data[col] = transformed_data[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        
        elif types[col] == "date":
            transformed_data[col] = pd.to_datetime(transformed_data[col], errors='coerce')
            transformed_data[col] = transformed_data[col].dt.strftime('%Y-%m-%d')

        elif types[col] == "integer":
            transformed_data[col] = transformed_data[col].astype(int)

        elif types[col] == "boolean":
            transformed_data[col] = transformed_data[col].astype(bool)
        
        else:
            transformed_data[col] = transformed_data[col].astype(str)
            transformed_data[col] = transformed_data[col].replace('', None)
            

    transformed_data.columns = [col.upper() for col in transformed_data.columns]
    transformed_data.to_csv(output_processsed_path, index=False)

    return output_processsed_path, types



# def update_to_snowflake(df, table_name, key_name):
#     """
#     Updates existing rows in a Snowflake table using a temporary table.
#     """
#     temp_table = f"{table_name}_TEMPORARY_TABLE".upper()
#     hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
#     conn = hook.get_conn()
#     cursor = conn.cursor()

#     try:
#         # Step 1: Create a temporary table
#         cursor.execute(f"""
#             CREATE OR REPLACE TEMPORARY TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} (
#                 {', '.join([f'"{col}" STRING' for col in df.columns])}
#             );
#         """)

#         # Step 2: Upload DataFrame to the temporary table
#         write_pandas(conn, df, temp_table, database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA, chunk_size=10000)

#         # Step 3: Construct the UPDATE SQL query
#         update_columns = [f'"{col}" = s."{col}"' for col in df.columns if col != key_name]
#         logger.info(f"update_columns: {update_columns}")
#         update_stmt = f"""
#             UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name} t
#             SET {', '.join(update_columns)}
#             FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{temp_table} s
#             WHERE s."{key_name}" = t."{key_name}";
#         """

#         cursor.execute(update_stmt)
#         conn.commit()
#         logger.info(f"Update executed successfully on {table_name}")

#     except Exception as e:
#         logger.error(f"Error executing update query: {e}")
#         conn.rollback()
#         raise

#     finally:
#         cursor.close()

# def handle_updates(source_data_path, destination_data_table):
#     """
#     Identifies new and modified rows between source and destination data.
#     Returns:
#         - inserts: DataFrame of new rows to be inserted.
#         - modified: DataFrame of rows to be updated.
#         - metadata: Dictionary containing metadata about the incremental load.
#     """
#     hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

#     if destination_data_table == "res.partner":
#         table_name = "contacts"
#     elif destination_data_table == "account.move":
#         table_name = "journal.entry"
#     elif destination_data_table == "account.move.line":
#         table_name = "journal.item"
#     elif destination_data_table == "sr.pdc.payment":
#         table_name = "pdc.payment"
#     else:
#         table_name = destination_data_table

#     table = table_name.replace('.', '_')
#     table_name = table.upper()
#     destination_data = hook.get_pandas_df(f"SELECT * EXCLUDE INSERT_DATE FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table_name}")

    
#     source_data = pd.read_csv(source_data_path)

#     for df in [source_data, destination_data]:
#         df.columns = df.columns.str.upper()
#         df["ID"] = df["ID"].astype(str).str.strip()

#     # Identify new rows (IDs not in destination)
#     existing_ids = set(destination_data["ID"]) if not destination_data.empty else set()
#     new_rows = source_data[~source_data["ID"].isin(existing_ids)]

#     # Identify modified rows (IDs present in both but with different UPDATED_AT values)
#     common_ids = source_data[source_data["ID"].isin(existing_ids)]
#     merged = common_ids.merge(
#         destination_data, 
#         on="ID", 
#         suffixes=('_source', '_dest')
#     )

#     modified_mask = (
#         merged["__LAST_UPDATE_source"].fillna('@@NULL@@') != merged["__LAST_UPDATE_dest"].fillna('@@NULL@@')
#     )

#     # Get all source columns from merged (ending with '_source')
#     source_cols_in_merged = [col for col in merged.columns if col.endswith('_source')]

#     # Select modified rows and rename columns to original names
#     modified_rows = merged[modified_mask][['ID'] + source_cols_in_merged].rename(
#         columns=lambda x: x.replace('_source', '') if x.endswith('_source') else x
#     )


#     # # Identify changes
#     # changes = source_data[~source_data.apply(tuple, 1).isin(destination_data.apply(tuple, 1))]
#     # inserts = changes[~changes.ID.isin(destination_data.ID)]
#     # modified = changes[changes.ID.isin(destination_data.ID)]

#     # Prepare metadata
#     metadata = {
#         "table_name": table_name,
#         "new_rows_count": len(new_rows),
#         "updated_rows_count": len(modified_rows),
#         "total_rows_processed": len(new_rows) + len(modified_rows),
#         "load_timestamp": datetime.now().strftime(DATE_FORMAT),
#     }

#     logger.info(f"New rows: {metadata['new_rows_count']}, Updated rows: {metadata['updated_rows_count']}")
#     return new_rows, modified_rows, metadata

def map_dtypes_to_snowflake(column, types):

    src_type = types[column.lower()]

    if src_type in ('datetime'):
        return 'TIMESTAMP_NTZ'
    
    elif src_type == 'time':
        return 'TIME'
    
    elif src_type == 'date':
        return 'DATE'
    
    elif src_type == 'boolean':
        return 'BOOLEAN'
    
    elif src_type in ('integer'):
        return 'NUMBER'
    
    elif src_type in ('float', 'double'):
        return 'FLOAT'
    
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


def log_load_metadata(metadata, dag_start_time, dag_finish_time, processing_time_sec, load_type, status="SUCCESS", error_message=None):
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

def load_data_to_snowflake(target_table, processed_data, **kwargs):
    """
    Loads data into Snowflake, handling both full and incremental loads.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    conn = hook.get_conn()
    metadata = {}
    
    if target_table == "res.partner":
        table_name = "contacts"
    elif target_table == "account.move":
        table_name = "journal.entry"
    elif target_table == "account.move.line":
        table_name = "journal.item"
    elif target_table == "sr.pdc.payment":
        table_name = "pdc.payment"
    else:
        table_name = target_table

    table = table_name.replace('.', '_')

    # Capture DAG start time (execution_date)
    dag_start_time = kwargs['execution_date']
    if dag_start_time.tzinfo is not None:
        dag_start_time = dag_start_time.replace(tzinfo=None)  # Strip timezone information
    logger.info(f"DAG start time: {dag_start_time}")

    try:
        if is_incremental:
            # Incremental load
            data_path, types = processed_data[0], processed_data[1]
            df = pd.read_csv(data_path)
            rows_inserted, rows_updated = merge_to_snowflake(df, table.upper(), "ID", snowflake_conn_id, types)

            # Update metadata for incremental load
            metadata.update({
                "table_name": table.upper(),
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

            data_path = processed_data[0]
            df = pd.read_csv(data_path, chunksize=chunksize)

            # Read the CSV file in chunks
            for chunk in df:
                # Insert the chunk into Snowflake
                write_pandas(conn, chunk, table.upper(), database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA)
                
                # Update the count of rows inserted (this could be logged or tracked)
                rows_inserted += len(chunk)
                print(f"Inserted {rows_inserted} rows so far.")
            
            # write_pandas(conn, df, target_table.upper(), schema=target_schema)
            # Update metadata for full load
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


def compare_stats(odoo_data_path,sf_data_path):
    compare_output_path = './data/raw/odoo/compare.csv'

    df_odoo = pd.read_csv(odoo_data_path)
    df_snowflake = pd.read_csv(sf_data_path)
    
    # Case normalization
    df_odoo['TABLE_NAME'] = df_odoo['TABLE_NAME'].str.upper()
    df_snowflake['TABLE_NAME'] = df_snowflake['TABLE_NAME'].str.upper()
    
    comparison = pd.merge(df_odoo, df_snowflake, on='TABLE_NAME', how='outer')
    
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
        'HR': HR_MODULE,
        'POS': POS_MODULE,
        'CUSTOMERS': CUSTOMERS_MODULE,
        'ACCOUNTING': ACCOUNTING_MODULE,
        'PURCHASE': PURCHASE_MODULE,
        'HELPDESK': HELPDESK_MODULE,
        'SALE': SALE_MODULE
    }
    SOURCE_NAME = 'ODOO'
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
        if table_lower == "contacts":
            table_name = "res.partner"
        elif table_lower == "journal.entry":
            table_name = "account.move"
        elif table_lower == "journal.item":
            table_name = "account.move.line"
        elif table_lower == "pdc.payment":
            table_name = "sr.pdc.payment"
        else:
            table_name = table_lower

        table = table_name.replace('_', '.')

        module_name = 'UNKNOWN'
        for mod, tables in module_mapping.items():
            if table in [t.lower() for t in tables]:
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