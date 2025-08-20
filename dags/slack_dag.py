from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta 
from slack_sdk import WebClient
from pendulum import timezone

local_tz = timezone("Asia/Seoul")
log = LoggingMixin().log
today = datetime.today()
today_str = today.strftime("%Y-%m-%d")

try: 
    SLACK_KEY = Variable.get('SLACK_KEY')
    log.info(f"SLACK_KEY를 가져오는데 성공 했습니다 : {SLACK_KEY}")
except Exception as e:
    log.info("SLACK_KEY를 가져오는데 실패했습니다.")

client = WebClient(token=SLACK_KEY)
dag_owner = 'Jack'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 4,
        'retry_delay': timedelta(seconds=3)
        }

with DAG(dag_id='slack',
        schedule='15 8 * * *',
        default_args=default_args,
        start_date=datetime(2025,8,18, tzinfo=local_tz),
        catchup=False,
        tags=['test']
):


    @task
    def send_msg():
        linear_score = float(Variable.get("linear_score"))
        lasso_score = float(Variable.get("lasso_score"))
        ridge_score = float(Variable.get("ridge_score"))

        result_dict = {"linear_score": linear_score,
                        "lasso_score": lasso_score,
                        "ridge_score": ridge_score}

        min_result_key = min(result_dict, key=result_dict.get)
        min_result_value = result_dict[min_result_key]

        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📊 {today_str} 모델 성능 요약*"
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Linear Regression:*\n{linear_score:.8f}"},
                    {"type": "mrkdwn", "text": f"*Ridge Regression:*\n{ridge_score:.8f}"},
                    {"type": "mrkdwn", "text": f"*Lasso Regression:*\n{lasso_score:.8f}"}
                ]
            },
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"ML 평가 결과 {min_result_key}가 {min_result_value}로 가장 좋은 성능을 기록했습니다."}]}
        ]

        client.chat_postMessage(channel="#새-채널", blocks=blocks)
        return ''

    send_msg()