from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime, timedelta 
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
from pendulum import timezone

local_tz = timezone("Asia/Seoul")

dag_owner = 'Jack'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5)
        }
###########
log = LoggingMixin().log

def ready_to_ml(df):
    X = df[['open', 'low','close','volume']]
    y = df['high']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

with DAG(dag_id='ML',
        default_args=default_args,
        schedule="10 8 * * *",
        start_date=datetime(2025,8,18, tzinfo=local_tz),

        catchup=False,
        tags=['test']
):
    @task
    def read_cleaned_data():
        PostgresHook_hook = PostgresHook(postgres_conn_id='pg_conn')
        read_sql = """
            SELECT * FROM cleaned_stock_info;
        """
        conn = PostgresHook_hook.get_conn()
        df = pd.read_sql(read_sql, conn)
        return df


    @task(retries=5, retry_delay=timedelta(seconds=2))
    def linear(df):
        X_train, X_test, y_train, y_test = ready_to_ml(df)
        lr = LinearRegression()
        lr.fit(X_train, y_train)
        y_pred_lr = lr.predict(X_test)
        log.info(f"LinearRegression MSE:{mean_squared_error(y_test, y_pred_lr)}")
        lr_score = mean_squared_error(y_test, y_pred_lr)
        Variable.set("linear_score", str(lr_score))
        return lr_score
    
    @task(retries=5, retry_delay=timedelta(seconds=2))
    def ridge(df):
        X_train, X_test, y_train, y_test = ready_to_ml(df)
        ridge = Ridge(alpha=1.0)
        ridge.fit(X_train,y_train)
        y_pred_ridge = ridge.predict(X_test)
        log.info(f"Ridge MSE:{mean_squared_error(y_test, y_pred_ridge)}")
        ridge_score = mean_squared_error(y_test, y_pred_ridge)
        Variable.set("ridge_score", str(ridge_score))
        return ridge_score

    @task(retries=5, retry_delay=timedelta(seconds=2))
    def lasso(df):
        X_train, X_test, y_train, y_test = ready_to_ml(df)
        lasso = Lasso(alpha=0.1)
        lasso.fit(X_train,y_train)
        y_pred_lasso = lasso.predict(X_test)
        log.info(f"Lasso MSE:{mean_squared_error(y_test, y_pred_lasso)}")
        lasso_score = mean_squared_error(y_test, y_pred_lasso)
        Variable.set("lasso_score", str(lasso_score))

        return lasso_score
    
    cleaned_df = read_cleaned_data()   

    # 모델 학습 태스크 정의 및 연결
    linear_task = linear(cleaned_df)          # 5) LinearRegression 학습 및 평가
    ridge_task = ridge(cleaned_df)            # 6) Ridge 학습 및 평가
    lasso_task = lasso(cleaned_df)            # 7) Lasso 학습 및 평가

    # 의존성 연결: cleaned_df 완료 후 세 모델 학습 태스크 병렬 실행
    [linear_task, ridge_task, lasso_task]