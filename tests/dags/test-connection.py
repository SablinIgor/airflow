import csv
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'i.sablin',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

def fetch_from_source():
    logging.info('Data Fetching...')
    hook = PostgresHook(postgres_conn_id="POSTGRESQL_SOURCE")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select 1")
    logging.info(cursor.description)
    for row in cursor:
        logging.info(row)
    cursor.close()
    conn.close()

with DAG(
    dag_id="test_connection",
    default_args=default_args,
    start_date=datetime(2023, 3, 3),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="fetch_from_devmon",
        python_callable=fetch_from_source
    )
    task1