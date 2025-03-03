# 226_hw5: porting hw4 to airflow

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta, datetime
import snowflake.connector
import requests
import json


def return_snowflake_conn():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
   
    return conn.cursor()
    


@task
def extract(symbol, api_key):
    url_template = Variable.get('stock_url')
    url = url_template.replace("{symbol}", symbol).replace("{alpha_vantage_api_key}", api_key)
    f = requests.get(url)
    return(f.text)
  
  
@task
def transform(data):
    # If data is a string, convert it to a dictionary
    if isinstance(data, str):
        data = json.loads(data)

    # Extract time series data
    time_series = data.get("Time Series (Daily)", {})
    symbol = data.get("Meta Data", {}).get("2. Symbol")

    records = []
    for date, values in time_series.items():
      open_price = values.get("1. open")
      high_price = values.get("2. high")
      low_price = values.get("3. low")
      close_price = values.get("4. close")
      volume = values.get("5. volume")
      records.append([symbol, date, open_price, high_price, low_price, close_price, volume])

    return records


@task
def load(records, target_table): #load_v2
    con = return_snowflake_conn()

    try:
        con.execute("BEGIN;")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
            symbol string,
            date timestamp,
            open number(38, 4),
            close number(38, 4),
            high number(38, 4),
            low number(38, 4),
            volume number(38, 0),
            PRIMARY KEY (symbol, date)
          )""")

        con.execute(f"""DELETE FROM {target_table}""")

        for r in records:
          symbol = r[0]
          date = r[1]
          open = r[2]
          high = r[3]
          low = r[4]
          close = r[5]
          volume = r[6]

          sql = f'''
                  INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                  VALUES ('{symbol}', '{date}', '{open}', '{high}', '{low}', '{close}', '{volume}')
                '''
          con.execute(sql)
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


default_args = {
   'owner': 'airflow',
   'retries': 1,
   'retry_delay': timedelta(minutes=3),
}



with DAG(
    dag_id = '226_hw5',
    default_args = default_args,
    start_date = datetime(2025,2,20),
    catchup = False,
    tags=['ETL'],
    schedule = '*/5 * * * *'    #runs every 5 minutes for testing
)as dag:
  symbol = 'PYPL'  # symbol for paypal holdings inc
  alpha_vantage_api_key = Variable.get("alpha_vantage_api_key")
  target_table = "dev.raw.stock"
  data = extract(symbol, alpha_vantage_api_key)
  lines = transform(data)
  load_task = load(lines, target_table)

data >> lines >> load_task
