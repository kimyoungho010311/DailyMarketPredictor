
[노션으로 보기](https://harsh-cabbage-818.notion.site/24904fe2aebc80bc9452e83db568a0ba?source=copy_link) 


이 프로젝트는 `yfinance` API로 미국 주식 데이터를 매일 수집하고 `PostgreSQL` 적재, 전처리, 간단한 ML 모델 학습, Slack 자동 알림까지 연결되는 End-to-End 자동화 파이프라인 구축을 목표로 했습니다.


# DAG

이 프로젝트는 총 4개의 DAG로 이루어져 있습니다. 각각 `stock`, `preprocess`, `ML`, `slack` 순서대로 파이프라인이 작동됩니다. 각각 DAG의 기능은 아래에 간략히 정리되어 있습니다.

- `stock` : 주식정보를 수집, DB 테이블 생성
- `preprocess` : 수집된 주식정보를 전처리, DB에 저장
- `ML` : DB에 저장된 데이터로 학습
- `slack` : 학습된 결과를 slack에서 자동으로 메세지 출력

아래 설명은 각 DAG별 중요한 로직은 자세히 설명하고, 다른 로직들은 간단히 설명후 넘어갑니다.

## Stock

### yfinance

야후에서 지원하는 `yfinance`를 사용하여 간단하게 주식정보를 수집할 수 있습니다. 

```python
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
```

위와 같이 `ticker_list` 에 수집할 주식들의 ticker를 리스트로 만들어 반복문을 통해 정보들을 받아옵니다.

받아온 데이터는 `DataFrame` 형태로 이루어져 있지만 컬럼명이 전저리하기 불편하기에 `rename_col` 리스트를 통해 원하는 컬럼명으로 수정하였습니다.

나머지 Task에 대한 설명은 아래와 같습니다.

- `create_stock_info_table`  : 원본 주식 데이터를 저장할 테이블을 생성합니다.
- `create_cleaned_stock_table` : 전처리된 주식 데이터를 저장할 테이블을 생성합니다.
- `insert_stock_info_to_db` : 원본 주식 데이터를 DB에 저장합니다.

<img width="938" height="326" alt="스크린샷 2025-08-20 오전 11 13 22" src="https://github.com/user-attachments/assets/5b6208f0-9d24-4021-a3fc-dfa369c511ba" />

## Preprocess



이 DAG는 원본 주식 데이터를 전처리 하는 DAG입니다.

이 프로젝트의 주요 목적은 파이프라인 구축, 스케쥴링이기 때문에 전처리를 하는 코드는 최대한 간단히 하였습니다.

각 Task별 설명은 아래와 같습니다.

- `read_data` : 전처리를 하기위한 데이터를 불러옵니다. 불러오는 기간은 어제 하루동안 수집된 주식 데이터만을 불러옵니다.

```python
from datetime import datetime, timedelta 

		today = datetime.today()
		yesterday = today - timedelta(days=1)
	
		today_str = today.strftime("%Y-%m-%d")
		yesterday_str = yesterday.strftime("%Y-%m-%d")

    @task
    def read_data():
        PostgresHook_hook = PostgresHook(postgres_conn_id='pg_conn')
        read_sql = f"""
            SELECT * FROM stock_info
            WHERE date::date = %s;    
        """
        # date 포멧을 맞추기 위해 :: 를 사용합니다.
        
        conn = PostgresHook_hook.get_conn()
        df = pd.read_sql(read_sql, conn, params=[yesterday_str])
        log.info(f"yesterday is : {yesterday}")
        log.info(f"yesterday_str is : {yesterday_str}")
        return df
```

- `preprocess` : 전처리를 하는 Task 입니다. 간단하게 Min-Max 스케일링을 합니다.
- `insert_cleaned_stock` : 전처리 완료한 데이터를 DB에 입력합니다.

<img width="932" height="251" alt="스크린샷 2025-08-20 오전 11 14 00" src="https://github.com/user-attachments/assets/01cbca6a-19ac-4c91-b208-3edfb196cd24" />

## ML

머신러닝을 담당하는 DAG입니다. 간단하게 Linear, Ridge, Lasso 모델을 사용했습니다.

각 Task별 설명은 아래와 같습니다.

- `read_cleaned_data` : 전처리 완료된 데이터를 불러옵니다.
- `linear`, `ridge`, `lasso` : 각 모델별로 학습을 담당합니다.

아래와같이 `cleaned_df`를 입력받아 학습을 하며 의존성을 연결하였습니다.

```python
    # 모델 학습 태스크 정의 및 연결
    linear_task = linear(cleaned_df)          # 5) LinearRegression 학습 및 평가
    ridge_task = ridge(cleaned_df)            # 6) Ridge 학습 및 평가
    lasso_task = lasso(cleaned_df)            # 7) Lasso 학습 및 평가

    # 의존성 연결: cleaned_df 완료 후 세 모델 학습 태스크 병렬 실행
    [linear_task, ridge_task, lasso_task]
```
<img width="872" height="589" alt="스크린샷 2025-08-20 오전 11 14 17" src="https://github.com/user-attachments/assets/8416f319-33c3-4704-a256-a5a67bbbd9de" />

## Slack

Slack에서 메세지를 보내는 DAG입니다. Slack과 연동을 하기위한 정보는 아래 블로그들을 참고 하였습니다.

[Apache 에어플로우(Airflow) 예제(example) - slack으로 알람, 메세지 받기](https://lsjsj92.tistory.com/634)

[Slack python api 연동 ( Error : channel_not_found 해결하기 )](https://m.blog.naver.com/kmh03214/222402397283)

Slack은 아래 Task 로 이루어져 있습니다.

- `send_msg` : Slack에서 메세지를 보내는 Task 입니다.

<img width="660" height="397" alt="스크린샷 2025-08-20 오전 11 14 41" src="https://github.com/user-attachments/assets/bc5b4892-2044-49d1-9ce8-34bbecc90ca2" />

위 사진과 같이 매일 아침 정해진 시간에 ML 학습 결과를 전송함을 확인하였습니다.

## 기타

Airflow에서 스케쥴을 설정할 때 기준이 되는 시간이 한국에 정해진 시간이 아님을 확인하였습니다. 기본값은 `utc` 였으며 이는 airflow.cfg 에서 설정 가능 하였습니다.

```docker
# Variable: AIRFLOW__CORE__DEFAULT_TIMEZONE
#
default_timezone = utc
```

위 `utc` 값을 `Asia/Seoul`로 변경하면 정상적으로 설정됩니다.

하지만 저는 다른 방법도 사용해 보았습니다. 각 DAG별로 아래와 같은 코드를 사용했습니다.

```python
from pendulum import timezone

local_tz = timezone("Asia/Seoul")

with DAG(dag_id='ML',
        default_args=default_args,
        schedule="10 8 * * *",
        start_date=datetime(2025,8,18, tzinfo=local_tz), #<-----

        catchup=False,
        tags=['test']
):
```

위와 같이 각 DAG별로 `local_tz`을 설정하면 Docker를 따로 설정하거나 재시작 할 필요가 없지만 나중에 유지보수하는데 매우 어려움을 줄것으로 예상되므로 이 방법은 사용하지 않는게 좋다고 생각합니다.

### 부족한 점

- 아직 DAG별 의존성을 설정하는데 어려움을 겪고 있습니다. 이는 추가적인 학습이 필요하다고 생각합니다.
- Task파일들의 디렉토리를 유지보수하기 쉽게 하고싶지만 아직 방법을 몰라 이 부분도 학습이 필요합니다.

### 배운 점

- Airflow DAG 구조 및 Task 의존관계 개념 이해
- Docker환경에서  전체 파이프라인 구축 경험 획득
- 실제 데이터 수집 → 전처리 → 모델링 → 결과 전달까지 자동화 경험
- DAG별로 역할을 명확히 분리하여 유지보수성 향상
