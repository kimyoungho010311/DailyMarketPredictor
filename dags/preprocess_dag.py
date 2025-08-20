from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta 
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from pendulum import timezone

local_tz = timezone("Asia/Seoul")
today = datetime.today()
yesterday = today - timedelta(days=1)

today_str = today.strftime("%Y-%m-%d")
yesterday_str = yesterday.strftime("%Y-%m-%d")

log = LoggingMixin().log
dag_owner = 'Jack'


default_args = {'owner': dag_owner,
        }

with DAG(dag_id='Preprocess',
        schedule="5 8 * * *", 
        default_args=default_args,
        start_date=datetime(2025, 8, 18,tzinfo=local_tz),
        catchup=False,
        tags=['test']
):

    @task
    def read_data():
        PostgresHook_hook = PostgresHook(postgres_conn_id='pg_conn')
        read_sql = f"""
            SELECT * FROM stock_info
            WHERE date::date = %s;
        """
        conn = PostgresHook_hook.get_conn()
        df = pd.read_sql(read_sql, conn, params=[yesterday_str])
        log.info(f"yesterday is : {yesterday}")
        log.info(f"yesterday_str is : {yesterday_str}")
        return df
    
    @task
    def read_cleaned_data():
        PostgresHook_hook = PostgresHook(postgres_conn_id='pg_conn')
        read_sql = """
            SELECT * FROM cleaned_stock_info;
        """
        conn = PostgresHook_hook.get_conn()
        df = pd.read_sql(read_sql, conn)
        return df

    @task
    def preprocess(df):
        df.dropna(inplace=True)
        # 스케일링할 컬럼
        cols_to_scale = ["open","high","low","close","volume"]

        # Min-Max 스케일링
        scaler = MinMaxScaler()
        df[cols_to_scale] = scaler.fit_transform(df[cols_to_scale])
        return df
    
    @task
    def insert_cleaned_stock(df):
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        insert_sql = """
        INSERT INTO cleaned_stock_info (date, open, high, low, close, volume, ticker)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        records = df[['date','open','high','low','close','volume','ticker']].values.tolist()

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        execute_batch(cursor, insert_sql, records)

        conn.commit()
        cursor.close()
        conn.close()



    # DAG 실행 순서 정의
    df_task = read_data()                      # 1) Postgres에서 원본 stock_info 테이블 읽기
    preprocessed_df = preprocess(df_task)      # 2) 결측치 제거 + MinMax 스케일링
    cleaned_task = insert_cleaned_stock(preprocessed_df)  # 3) 전처리 데이터 cleaned_stock_info 테이블에 삽입
    cleaned_df = read_cleaned_data()           # 4) DB에 삽입된 데이터를 다시 읽어 DataFrame으로 반환

    # 의존성 연결
    preprocessed_df >> cleaned_task >> cleaned_df  
    # - preprocessed_df 완료 후 cleaned_task 실행
    # - cleaned_task 완료 후 cleaned_df 실행 (DB 삽입 후 읽기 보장)


    # 의존성 연결: cleaned_df 완료 후 세 모델 학습 태스크 병렬 실행
    cleaned_df
