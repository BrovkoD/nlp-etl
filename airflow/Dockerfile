FROM apache/airflow:2.8.1-python3.10

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["bash", "-c", "\
airflow db check \
|| (airflow db init && airflow users create \
    --username ${AIRFLOW_USERNAME} \
    --firstname ${AIRFLOW_FIRSTNAME} \
    --lastname ${AIRFLOW_LASTNAME} \
    --role ${AIRFLOW_ROLE} \
    --email ${AIRFLOW_EMAIL} \
    --password ${AIRFLOW_PASSWORD}) \
&& exec \"$@\"", "--"]