import os
import pandas as pd
from livepopulartimes import get_populartimes_by_address
from datetime import datetime, timedelta
from time import sleep
import time
import json
import logging
import pytz

from scripts.scrape_services_data import places_seach, location
from scripts.preprocess_service_data import preprocess_data

Saigon_timezone = pytz.timezone('Asia/Saigon')

basic_attributes = ['name', 'formatted_address']
live_attributes = ['place_id', 'name', 'datetime', 'rating', 'rating_n',
                   'populartimes', 'usual_popularity', 'current_popularity']

def get_basic_data(file_path, attributes):
    df = pd.read_csv(file_path)

    return df.loc[:, attributes].to_dict()


def map_weekday(origin):
    if origin:
        return origin - 1
    return 6


def get_live_data(basic_file_path, basic_attributes, attributes, write_csv=False):
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    basic_data_dict = get_basic_data(basic_file_path, basic_attributes)

    # define live data dictionary
    live_data_dict = {attr: [] for attr in attributes}
    live_data_dict.update({'datetime': []})
    name_dict, addr_dict = basic_data_dict[basic_attributes[0]], basic_data_dict[basic_attributes[1]]

    # get datetime
    dt_obj = datetime.now()
    dt = dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    weekday = int(dt_obj.strftime('%w'))
    hour = int(dt_obj.strftime('%H'))

    # not live objects
    not_livetime_places = []

    # get live data
    name_dict_first10 = {k: name_dict[k] for k in list(name_dict)[:5]}
    addr_dict_first10 = {k: addr_dict[k] for k in list(addr_dict)[:5]}
    for name, addr in zip(list(name_dict_first10.values()), list(addr_dict_first10.values())):
        live_response = get_populartimes_by_address(f'({name}) {addr}')
        
        live_data_record = {}#get each record send to kafka

        if 'populartimes' in live_response:
            live_data_record['datetime'] = dt
            live_data_record['usual_popularity'] = live_response['populartimes'][map_weekday(weekday)]['data'][hour]
        else:
            not_livetime_places.append(name)
            continue

        for attr in attributes:
            if attr not in ['datetime', 'usual_popularity','populartimes']:
                live_data_record[attr] = live_response.get(attr, None)

        for day in live_response['populartimes']:
            live_data_record[day['name'].lower()] = day['data']

        try:
            producer.send('live_service', json.dumps(live_data_record).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')

    return not_livetime_places

def get_next_crawling_time():
    now = datetime.now(Saigon_timezone)
    next_time = now + timedelta(minutes=15)
    next_time = datetime(next_time.year, next_time.month, next_time.day,
                         next_time.hour, (next_time.minute // 15) * 15, 1,
                         tzinfo=Saigon_timezone)

    return next_time

def live_service_stream():

    # check if scraping basic data yet
    if not os.path.isfile(r'/opt/airflow/dags/data/services.csv'):
        service_list = places_seach(progress_file_path=r'/opt/airflow/dags/data/searching_progress.txt',
                                    res_file_path=r'/opt/airflow/dags/data/services.json',
                                    max_pages=10)
        preprocess_data(r'/opt/airflow/dags/data/services.json',
                        included_pattern=r'(Cách Mạng Tháng 8|CMT8).*(Hồ Chí Minh|HCM)',
                        file_out=r'/opt/airflow/dags/data/services.csv')

    while True:
        next_time = get_next_crawling_time()
        if datetime.now(Saigon_timezone).minute != next_time.minute:
           sleep((next_time + timedelta(minutes=7) - datetime.now(Saigon_timezone)).seconds)

        try:
            not_live_places = get_live_data(r'/opt/airflow/dags/data/services.csv',
                                                   basic_attributes=basic_attributes,
                                                   attributes=live_attributes,
                                                   write_csv=True)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            break

