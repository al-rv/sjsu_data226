from airflow import DAG
#from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import snowflake.connector

# Establishing connection to Snowflake
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


# Define SQL statements
CREATE_TABLE_USER_SESSION_CHANNEL = """
CREATE TABLE IF NOT EXISTS raw.user_session_channel (
    userId INT NOT NULL,
    sessionId VARCHAR(32) PRIMARY KEY,
    channel VARCHAR(32) DEFAULT 'direct'
);
"""

CREATE_TABLE_SESSION_TIMESTAMP = """
CREATE TABLE IF NOT EXISTS raw.session_timestamp (
    sessionId VARCHAR(32) PRIMARY KEY,
    ts TIMESTAMP
);
"""

CREATE_STAGE = """
CREATE OR REPLACE STAGE raw.blob_stage
URL = 's3://s3-geospatial/readonly/'
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
"""

COPY_USER_SESSION_CHANNEL = """
COPY INTO raw.user_session_channel
FROM @raw.blob_stage/user_session_channel.csv;
"""

COPY_SESSION_TIMESTAMP = """
COPY INTO raw.session_timestamp
FROM @raw.blob_stage/session_timestamp.csv;
"""
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define DAG
with DAG(
    dag_id="snowflake_s3_etl",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 20),
    tags=['s3_ETL'],
    default_args=default_args,
    catchup=False,
) as dag:

    @task()
    def create_tables():
        """Create Snowflake tables if they do not exist."""
        cursor = return_snowflake_conn()
        queries = [CREATE_TABLE_USER_SESSION_CHANNEL, CREATE_TABLE_SESSION_TIMESTAMP]
        for query in queries:
            cursor.execute(query)

    @task()
    def create_s3_stage():
        """Create a Snowflake external stage for loading data from S3."""
        cursor = return_snowflake_conn()
        cursor.execute(CREATE_STAGE)

    @task()
    def copy_data():
        """Copy data from S3 into Snowflake tables."""
        cursor = return_snowflake_conn()
        queries = [COPY_USER_SESSION_CHANNEL, COPY_SESSION_TIMESTAMP]
        for query in queries:
            cursor.execute(query)

    # Task Dependencies
    create_tables() >> create_s3_stage() >> copy_data()