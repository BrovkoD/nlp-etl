from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

load_dotenv()
log = logging.getLogger(__name__)

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

LLM_API_URL = os.getenv("SUMMARY_API_URL")


def load_data(**kwargs):
    engine = create_engine(DB_URI)
    df = pd.read_sql(
        """
        SELECT id, text
        FROM nlp_etl.input_data
        WHERE id NOT IN (SELECT input_id FROM predicted_data)
        LIMIT 5
        """, engine)
    kwargs['ti'].xcom_push(key='data', value=df.to_dict())

    log.info(df)


def predict(**kwargs):
    data = kwargs['ti'].xcom_pull(key='data', task_ids='extract_data')
    df = pd.DataFrame(data)
    summaries = []
    for _, row in df.iterrows():
        res = requests.post(LLM_API_URL, json={"text": row['text']})
        summaries.append(res.json().get("summary", ""))
    df['summary'] = summaries
    kwargs['ti'].xcom_push(key='result', value=df.to_dict())

    log.info(summaries)


def persist_result(**kwargs):
    result = kwargs['ti'].xcom_pull(key='result', task_ids='predict')
    df = pd.DataFrame(result)
    engine = create_engine(DB_URI)
    df = df.rename(columns={"id": "input_id"})
    df[["input_id", "summary"]].to_sql("predicted_data", engine, if_exists="append", index=False)

    log.info("done")


with DAG(
        dag_id="text_summary_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,  # daily: '0 7 * * *'
        catchup=False,
        tags=["NLP", "summary"],
) as dag:
    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=load_data,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="predict",
        python_callable=predict,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id="load_result",
        python_callable=persist_result,
        provide_context=True
    )

    t1 >> t2 >> t3
