from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from telethon.sync import TelegramClient
import requests
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_URI = (
        "mysql+pymysql://"
        + os.getenv("MYSQL_USER")
        + ":"
        + os.getenv("MYSQL_PASSWORD")
        + "@"
        + os.getenv("MYSQL_HOST")
        + "/"
        + os.getenv("MYSQL_DB")
)

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_CHANNEL = os.getenv("TELEGRAM_CHANNEL")

LLM_API_URL = os.getenv("MANIPULATION_API_URL")


def load_data(**kwargs):
    with TelegramClient('airflow_session', TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
        entity = client.get_entity(TELEGRAM_CHANNEL)
        message = client.get_messages(entity, limit=1)[0]
        kwargs['ti'].xcom_push(key='last_message', value=message.text)


def predict(**kwargs):
    message = kwargs['ti'].xcom_pull(key='last_message')
    payload = {"text": message}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(LLM_API_URL, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    result = response.json()
    kwargs['ti'].xcom_push(key='llm_result', value=json.dumps(result, ensure_ascii=False))


def persist_result(**kwargs):
    message = kwargs['ti'].xcom_pull(key='last_message')
    llm_result = kwargs['ti'].xcom_pull(key='llm_result')

    engine = create_engine(DB_URI)
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS telegram_entities (
                id INT AUTO_INCREMENT PRIMARY KEY,
                original_message TEXT,
                llm_result JSON,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            INSERT INTO telegram_entities (original_message, llm_result)
            VALUES (:message, :result)
        """), {"message": message, "result": llm_result})


with DAG(
        dag_id="telegram_custom_llm_to_db",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,  # daily: '0 7 * * *'
        catchup=False,
        tags=["NLP"],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_telegram_message',
        python_callable=load_data,
        provide_context=True,
        dag=dag
    )

    llm_task = PythonOperator(
        task_id='send_to_custom_llm',
        python_callable=predict,
        provide_context=True,
        dag=dag
    )

    db_task = PythonOperator(
        task_id='save_to_db',
        python_callable=persist_result,
        provide_context=True,
        dag=dag
    )

    fetch_task >> llm_task >> db_task
