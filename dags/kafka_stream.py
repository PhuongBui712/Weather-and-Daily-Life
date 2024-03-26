import requests
import uuid
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time 
from scripts.weather_stream import stream_data
from scripts.update_live_service import live_service_stream


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

        
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    weather_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

    live_service_task = PythonOperator(
        task_id='live_service_stream',
        python_callable=live_service_stream
    )

[weather_task, live_service_task]