import yfinance as yf
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from pendulum import timezone

local_tz = timezone("Asia/Seoul")
log = LoggingMixin().log
today = datetime.today()
yesterday = today - timedelta(days=1)

today_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")

with DAG(
    dag_id='stock',
    schedule="0 8 * * *",
    start_date=datetime(2025, 8, 19, tzinfo=local_tz),
    catchup=True,
    tags=['test']
) as dag:

    @task
    def load_stock():
        ticker_list = ['MSFT','AAPL','AMZN','GOOGL','TSLA']
        rename_col = ['open','high','low','close','volume']
        df_list = []
        for ticker in ticker_list:
            data = yf.download(ticker, start=yesterday, end=today)
            data.columns = rename_col
            data['ticker'] = ticker
            df_list.append(data)

        merged_df = pd.concat(df_list).reset_index()
        return merged_df
    
    @task
    def create_stock_info_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_info (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            date TIMESTAMP,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume INTEGER,
            ticker TEXT,
            UNIQUE (date, ticker)
        );  
        """
        pg_hook.run(create_table_sql)

    @task
    def created_cleaned_stock_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cleaned_stock_info (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            date TIMESTAMP,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume INTEGER,
            ticker TEXT
            )
        """
        pg_hook.run(create_table_sql)

    @task
    def insert_stock_info_to_db(merged_df):
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')

        insert_sql = """
        INSERT INTO stock_info (date, open, high, low, close, volume, ticker)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date, ticker) DO NOTHING;
        """

        records = merged_df[['Date','open','high','low','close','volume','ticker']].values.tolist()

        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        execute_batch(cursor, insert_sql, records)
        conn.commit()
        cursor.close()
        conn.close()

    create_table_task = create_stock_info_table()
    create_cleaned_task = created_cleaned_stock_table()
    merged_df_task = load_stock()
    insert_task = insert_stock_info_to_db(merged_df_task)

    [create_table_task, create_cleaned_task] >> merged_df_task >> insert_task