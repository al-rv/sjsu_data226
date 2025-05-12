from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        # Create a temporary table
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Check for duplicate rows (entire row uniqueness)
        duplicate_check_sql = f"""
          SELECT COUNT(*) AS total_rows, 
                 COUNT(DISTINCT userid, sessionid, channel, TS) AS distinct_rows
          FROM {database}.{schema}.temp_{table}
        """
        logging.info(duplicate_check_sql)
        cur.execute(duplicate_check_sql)
        result = cur.fetchone()
        total_rows, distinct_rows = result
        logging.info(f"Total rows: {total_rows}, Distinct rows: {distinct_rows}")

        if total_rows != distinct_rows:
            raise Exception(f"Duplicate rows detected. Total rows: {total_rows}, Distinct rows: {distinct_rows}")

        # Check for primary key uniqueness

        if primary_key is not None:
            primary_key_check_sql = f"""
              SELECT {primary_key}, COUNT(1) AS cnt 
              FROM {database}.{schema}.temp_{table}
              GROUP BY {primary_key}
              HAVING COUNT(1) > 1
              LIMIT 1
            """
            logging.info(primary_key_check_sql)
            cur.execute(primary_key_check_sql)
            result = cur.fetchone()
            if result:
                raise Exception(f"Primary key uniqueness failed for value: {result}")

        # Create main table if not exists
        main_table_creation_if_not_exists_sql = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} AS
            SELECT * FROM {database}.{schema}.temp_{table} WHERE 1=0
        """
        cur.execute(main_table_creation_if_not_exists_sql)

        # Swap tables
        swap_sql = f"ALTER TABLE {database}.{schema}.{table} SWAP WITH {database}.{schema}.temp_{table}"
        cur.execute(swap_sql)

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise



with DAG(
    dag_id = 'BuildELT_CTAS',
    start_date = datetime(2025, 3, 20),
    catchup=False,
    tags=['ELT'],
    schedule = '45 0 * * *'
) as dag:

    database = "USER_DB_BLUEJAY"
    schema = "ANALYTICS"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM dev.raw.user_session_channel u
    JOIN dev.raw.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId')