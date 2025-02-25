import os
import sys
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from itertools import chain

# Constants
CREDENTIALS_PATH = './config/credentials.json'
SCHEMA_NAME = 'APP'

SPORTS_TABLES = [
    'academy_assessments', 'academy_bookings', 'academy_booking_slots', 'academy_booking_subscriptions',
    'academy_customer_ratings', 'academy_edit_schedules', 'academy_level_ups', 'academy_ratings',
    'academy_sports', 'academy_waiting_lists', 'age_ranges',
    'age_range_levels', 'assessment_slots', 'canceled_slots', 'cancel_academy_bookings',
    'cancel_coaching_bookings', 'cancel_court_bookings', 'coaching_bookings' , 'coaching_booking_slots',
    'coaching_customer_ratings', 'coaching_packages', 'coaching_ratings', 'coaching_rating_questions',
    'coaching_slots', 'coaching_sports', 'coaching_sport_courts', 'courts', 'court_bookings',
    'court_booking_equipment', 'court_booking_invitations', 'court_booking_shares', 'court_booking_slots',
    'court_equipment', 'court_schedules', 'court_slots', 'court_sports', 'court_sport_courts',
    'customer_coaching_packages', 'level_groups', 'slots', 'sports','subscription_plans'
    # 'academy_sport_courts', 'coach_ratings'
]

DINING_TABLES = [
    'orders', 'order_deliveries', 'order_issues', 'order_products', 'order_product_extras',
    'order_product_options', 'order_ratings', 'order_rating_notifications', 'order_tracking', 'combos',
    'combo_products', 'delivery_mens', 'dining_banners', 'disabled_restaurants', 'extras', 'extra_products',
    'menu_categories', 'menu_category_products', 'products', 'product_options', 'product_option_items',
    'product_sizes', 'rating_questions', 'restaurants', 'restaurant_settings', 'sizes',
    'zones', 'sub_zones', 'waiter_logins', 'waiter_notifications'
    # 'waiter_rejected_orders', 'restaurant_tags'
]

ACCESS_TABLES = ['club_accesses', 'satra_pass_creation_logs', 'ticket_types', 'visitor_accesses']

NOTIFICATIONS_TABLES = ['notifications','notification_campaigns']

WALLETS_PAYMENT_TABLES = ['wallets', 'payments']

MEMBERS_TABLES = [
    'customers', 'customer_cards', 'customer_forget_pins', 'customer_logins',
    'customer_spending_limits', 'friendships', 'users', 'contact_messages', 'contact_message_subjects',
    'family_free_invitations'
    # 'customer_interest'
]

COMMUNITIES_TABLES = [
    'communities', 'community_community_level', 'community_customer_level', 'community_levels'
]

OTHER_TABLES = [
    'allowed_phones', 'cities', 'countries', 'interests', 'loyalty_cards', 'loyalty_programs'
]

# NON_UPDATED_TABLES = ['restaurant_tags', 'waiter_rejected_orders', 'coach_ratings', 'academy_sport_courts', 'customer_interest']

ALL_TABLES = [
    SPORTS_TABLES,
    DINING_TABLES,
    ACCESS_TABLES,
    NOTIFICATIONS_TABLES,
    WALLETS_PAYMENT_TABLES,
    MEMBERS_TABLES,
    COMMUNITIES_TABLES,
    OTHER_TABLES
]
ALL_TABLES = list(chain(*ALL_TABLES))
UPDATED_COLUMN = 'UPDATED_AT'

# Add the project root directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.app_pipeline import (
    META_COUNT_PATH,
    SF_META_COUNT_PATH,

    cleanup_file,
    load_json_file,
    app_table_info, 
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


credentials = load_json_file(CREDENTIALS_PATH)

config = credentials[0]
credential_id = config['credential_id']
mysql_conn_id = config['mysql_conn_id']
snowflake_conn_id = config['snowflake_id']
is_incremental = config['is_incremental']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12)
}

with DAG(
    'app_pipeline_automation',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
) as dag:
    
    cleanup_group = TaskGroup("cleanup_files_task")
    
    with cleanup_group:
        cleanup_path_task = PythonOperator(
            task_id='clean_path_task',
            python_callable=cleanup_file,
            op_args=[META_COUNT_PATH, SF_META_COUNT_PATH,],
            provide_context=True
        )

    with TaskGroup(group_id=credential_id, tooltip=f"Tasks for {credential_id}") as credential_group:
        for table in ALL_TABLES:

            # Task to check last load date (only for incremental tables)
            if is_incremental:
                check_last_load_date_task = PythonOperator(
                    task_id=f'check_last_load_date_{credential_id}_{table}',
                    python_callable=get_last_load_dates,
                    op_args=[snowflake_conn_id, table.upper()],
                    provide_context=True
                )


            # Task to extract data
            extract_task = PythonOperator(
                task_id=f'extract_data_{credential_id}_{table}',
                python_callable=extract_data,
                op_args=[mysql_conn_id, snowflake_conn_id, table, UPDATED_COLUMN, is_incremental],
                provide_context=True
            )

            # Set dependency for incremental check task
            if is_incremental:
                check_last_load_date_task >> extract_task


            # Task to transform data
            transform_task = PythonOperator(
                task_id=f'transform_data_{credential_id}_{table}',
                python_callable=transform_data,
                op_args=[extract_task.output, is_incremental],
                provide_context=True
            )
            extract_task >> transform_task

            # Task to load data into Snowflake
            load_task = PythonOperator(
                task_id=f'load_data_{credential_id}_{table}',
                python_callable=load_data_to_snowflake,
                op_args=[
                    snowflake_conn_id, 
                    is_incremental, 
                    table,
                    transform_task.output,
                    SCHEMA_NAME
                ],
                provide_context=True
            )
            transform_task >> load_task


            app_table_info_task = PythonOperator(
                task_id=f'app_table_info_task_{credential_id}_{table}',
                python_callable=app_table_info,
                op_args=[snowflake_conn_id, table.upper(), extract_task.output]
            )
            load_task >> app_table_info_task


    validation_group = TaskGroup("validation_tasks")
    
    with validation_group:
        # mysql_stats = PythonOperator(
        #     task_id='extract_mysql_stats',
        #     python_callable=extract_mysql_stats,
        #     op_kwargs={'extracted_data': META_COUNT_PATH}
        # )
        
        # snowflake_stats = PythonOperator(
        #     task_id='extract_snowflake_stats',
        #     python_callable=extract_snowflake_stats,
        #     op_kwargs={'extracted_data': SF_META_COUNT_PATH}
        # )
        
        compare_stats_task = PythonOperator(
            task_id='compare_stats',
            python_callable=compare_stats,
            provide_context=True,
            op_kwargs={
                'mysql_data_path': META_COUNT_PATH,
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
