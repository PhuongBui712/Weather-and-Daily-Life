import requests
import uuid
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time 

from scripts.weather_stream import live_weather
from scripts.update_live_service import live_service_stream
from scripts.traffic_stream import live_traffic


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

        
with DAG('pipeline_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    weather_task = PythonOperator(
        task_id = 'weather_stream',
        python_callable = live_weather
    )

    live_service_task = PythonOperator(
        task_id='live_service_stream',
        python_callable=live_service_stream
    )

    traffic_task = PythonOperator(
        task_id='traffic_stream',
        python_callable=live_traffic
    )

[weather_task, live_service_task,traffic_task]